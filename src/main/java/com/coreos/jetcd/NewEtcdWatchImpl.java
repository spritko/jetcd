package com.coreos.jetcd;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.coreos.jetcd.api.Event;
import com.coreos.jetcd.api.KeyValue;
import com.coreos.jetcd.api.ResponseHeader;
import com.coreos.jetcd.api.WatchCancelRequest;
import com.coreos.jetcd.api.WatchCreateRequest;
import com.coreos.jetcd.api.WatchGrpc;
import com.coreos.jetcd.api.WatchRequest;
import com.coreos.jetcd.api.WatchResponse;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.RevisionCompactedException;
import com.coreos.jetcd.watch.WatchCancelledException;
import com.coreos.jetcd.watch.WatchCreateException;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

/**
 * 
 * @author nickhill
 */
public class NewEtcdWatchImpl implements EtcdWatch, AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(NewEtcdWatchImpl.class);
    
    /* 
     * Internal rework in progress! Should be fully functional though
     */
    
    /* Watcher states:
     *   - in pendingCreate only, watchId < 0, finished == false
     *   - in activeWatchers only, watchId >= 0, finished == false
     *          (stream completion event published in this transition)
     *   - in neither, watchId < 0, finished = true
     */
    
    private final WatchGrpc.WatchStub watchStub;
    private final Executor observerExecutor; // "parent" executor
    private final Executor eventLoop; // serialized
    private final ScheduledExecutorService ses; // just for scheduling retries

    @GuardedBy("this")
    private StreamObserver<WatchRequest> requestStream;

    private final Queue<WatcherRecord> pendingCreate = new ConcurrentLinkedQueue<>();
    
    @GuardedBy("eventLoop")
    private final Map<Long,WatcherRecord> activeWatchers = new HashMap<>();
    
    
    public NewEtcdWatchImpl(ManagedChannel channel, Optional<String> token) {
        this(channel, token, null);
    }
    
    public NewEtcdWatchImpl(ManagedChannel channel, Optional<String> token, Executor executor) {
        this.watchStub = EtcdClientUtil.configureStub(WatchGrpc.newStub(channel), token);
        if(executor == null) executor = ForkJoinPool.commonPool();
        this.observerExecutor = executor;
        
        //TODO use same as grpc one
        this.eventLoop = new SerializingExecutor(executor, 128); // bounded for back-pressure
        this.ses = executor instanceof ScheduledExecutorService
                ? (ScheduledExecutorService)executor
                        : Executors.newSingleThreadScheduledExecutor();
    }
    
    /**
     * 
     */
    class WatcherRecord {

        private final StreamObserver<WatchEvent> observer;
        private final ByteString key;
        private final WatchOption options;
        private final Executor watcherExecutor;

        private WatchHandle creationFuture;
        
        long upToRevision;
        long watchId = -2L; // -2 only pre-creation, >= -1 after
        boolean userCancelled, finished;
        volatile boolean vUserCancelled;

        public WatcherRecord(ByteString key, WatchOption options,
                StreamObserver<WatchEvent> observer,
                Executor parentExecutor) {
            this.observer = observer;
            this.key = key;
            this.options = options;
            long rev = options.getRevision();
            this.upToRevision = rev - 1;
            // bounded for back-pressure
            this.watcherExecutor = new SerializingExecutor(parentExecutor, 64);
        }

        // null => cancelled (non-error)
        public void publishCompletionEvent(final Exception err) {
            watcherExecutor.execute(() -> {
                completeCreateFuture(false, err);
                try {
                    if(err == null || vUserCancelled) observer.onCompleted();
                    else observer.onError(err);
                } catch(Exception e) {
                    logger.warn("Watch "+watchId
                            +" observer onCompleted/onError threw", e);
                }
            });
        }

        @GuardedBy("eventLoop")
        public void processWatchEvents(WatchResponse wr) {
            if(userCancelled) return; // suppress events if cancelled
            final long newRevision = wr.getHeader().getRevision(), nowUpTo = upToRevision;
            if(wr.getEventsCount() > 0 && newRevision > nowUpTo) {
                watcherExecutor.execute(() -> {
                    ResponseHeader etcdHeader = wr.getHeader();
                    try {
                        for(Event event : wr.getEventsList()) {
                            if(vUserCancelled) break;
                            KeyValue kv = event.getKv();
                            if(kv != null && kv.getModRevision() <= nowUpTo) continue;
                            observer.onNext(new WatchEvent(etcdHeader,
                                    kv, event.getPrevKv(),
                                    mapEventType(event.getType())));
                        }
                    } catch(Exception e) {
                        //TODO "observer onNext threw", e
                        // must also cancel the watch here per StreamObserver contract
                        cancel();
                        //TODO this will result in the watcher receiving
                        // a final onComplete, but it should really be onError in this case
                    }
                });
            }
            upToRevision = newRevision; //TODO make sure this rev is appropriate
        }
        
        @GuardedBy("eventLoop")
        public void processCreatedResponse(WatchResponse wr) {
            long newWatchId = wr.getWatchId();
            if(wr.getCanceled() || newWatchId == -1L) {
                processCancelledResponse(wr);
            } else {
                boolean first = this.watchId == -2L;
                this.watchId = newWatchId;
                if(activeWatchers.putIfAbsent(newWatchId, this) == null) {
                    if(userCancelled) sendCancel(this);
                    else {
                        if(first) watcherExecutor.execute(() -> {
                            completeCreateFuture(true, null);
                            observer.onNext(new WatchEvent(wr.getHeader(),
                                    null, null, EventType.ESTABLISHED));
                        });
                        processWatchEvents(wr);

                        //TODO publish "caught up" event now?
                        //  based on seen revision being caught up to startrevision
                        // OR "current" rev being earlier than startrevision
                        // (may not have sent other events)
                    }
                } else {
                    logger.error("State error: watchId conflict: "+watchId);
                    //TODO other action TBD (probably cancel existing one)
                }
            }
        }

        @GuardedBy("eventLoop")
        public void processCancelledResponse(WatchResponse watchResponse) {
            watchId = -1L;
            if(finished) {
                logger.warn("Ignoring unexpected cancel response for watch "
                        +watchResponse.getWatchId());
                return;
            }
            finished = true;
            Exception error;
            if(userCancelled) error = null;
            else {
                ResponseHeader header = watchResponse.getHeader();
                long cRev = watchResponse.getCompactRevision();
                if(cRev != 0) error = new RevisionCompactedException(header, cRev);
                else if(watchResponse.getCreated()) error = new WatchCreateException(header);
                else error = new WatchCancelledException(header);
            }
            publishCompletionEvent(error);
        }

        @GuardedBy("eventLoop")
        public WatchRequest newCreateWatchRequest() {
            return optionToWatchCreateRequest(key, options, upToRevision +1);
        }
        
        // NOT guarded
        public void cancel() {
            if(closed || finished || userCancelled) return;
            eventLoop.execute(() -> {
                    if(closed || userCancelled || finished) return;
                    sendCancel(this);
                    vUserCancelled = userCancelled = true;
            });
        }
        
        private void completeCreateFuture(boolean created, Exception error) {
            WatchHandle wh = creationFuture;
            if(wh == null) return;
            wh.complete(created, error);
            creationFuture = null;
        }
    }
    
    @Override
    public Watch watch(ByteString key, WatchOption watchOption, StreamObserver<WatchEvent> observer) {
        Preconditions.checkNotNull(key, "key can't be null");
        Preconditions.checkNotNull(key, "observer can't be null");
        if(watchOption == null) watchOption = WatchOption.DEFAULT;
        if(closed) throw new IllegalStateException("closed");
        final WatcherRecord wrec = new WatcherRecord(key, watchOption, observer, observerExecutor);
        WatchRequest createReq = wrec.newCreateWatchRequest();
        WatchHandle handle = new WatchHandle(wrec);
        synchronized(this) {
            StreamObserver<WatchRequest> reqStream = getRequestStream();
            if(reqStream == null) throw new IllegalStateException("closed");
            pendingCreate.add(wrec);
            reqStream.onNext(createReq); //TODO can this throw? if so things would end up in a bad state
            System.out.println("sent new create request");
        }
        return handle;
    }
    
    static class WatchHandle extends AbstractFuture<Boolean> implements Watch {
        private final WeakReference<WatcherRecord> wrecRef;
        
        public WatchHandle(WatcherRecord wrec) {
            wrecRef = new WeakReference<>(wrec);
            wrec.creationFuture = this;
        }
        
        @Override
        public void close() {
            WatcherRecord wrec = wrecRef.get();
            if(wrec != null) wrec.cancel();
        }
        
        @Override
        protected void interruptTask() {
            close();
        }
        
        void complete(boolean created, Exception error) {
            if(error != null) setException(error);
            else set(created);
            set(created);
        }
    }
    
    @GuardedBy("eventLoop")
    protected void sendCancel(WatcherRecord wrec) {
        if(waiting) {
            wrec.finished = true;
            wrec.publishCompletionEvent(null);
            return;
        }
        
        if(wrec.watchId < 0) return;
        // send cancel request
        WatchRequest cancelReq = WatchRequest.newBuilder()
                .setCancelRequest(WatchCancelRequest.newBuilder()
                        .setWatchId(wrec.watchId).build()).build();
        synchronized(this) {
            // don't need to re-initialize reqstream if null (watch can't exist) <- TODO think about other simplifications from this
            StreamObserver<WatchRequest> reqStream = closed ? null : requestStream;
            if(reqStream != null) {
                reqStream.onNext(cancelReq);
                System.out.println("sent new cancel request");
            }
        }
    }
    
    @GuardedBy("this")
    protected StreamObserver<WatchRequest> getRequestStream() {
        if(closed) return null;
        if(requestStream == null) {
            requestStream = watchStub.watch(responseObserver); //TODO exceptions possible?
            System.out.println("established new watch/request stream");
        }
        return requestStream;
    }
    
    protected final StreamObserver<WatchResponse> responseObserver = new StreamObserver<WatchResponse>() {
        @Override
        public void onNext(WatchResponse wr) {
            System.out.println("RespObserver: onNext: "+wr);
            eventLoop.execute(() -> {
                errCounter = 0;
                processResponse(wr);
            });
        }
        @Override
        public void onCompleted() {
            System.out.println("RespObserver: onCompleted");
            dispatchStreamCompleted(true);
        }
        @Override
        public void onError(Throwable t) {
            System.out.println("RespObserver: onError: "+t);
            t.printStackTrace();
            if(!(t instanceof CancellationException)) {
                logger.warn("Watch response stream error", t);
            }
            dispatchStreamCompleted(true);
        }
    };

    @GuardedBy("eventLoop")
    protected void processResponse(WatchResponse wr) {
        boolean cancelled = wr.getCanceled() || wr.getCompactRevision() != 0;
        long watchId = wr.getWatchId();
        WatcherRecord wrec;
        if(wr.getCreated()) {
            wrec = pendingCreate.poll();
            if(wrec == null) {
                logger.error("State error: received unexpected watch create response: "+wr);
                //TODO here cancel and forget OR *maybe* close stream and refresh all
                throw new IllegalStateException();
            }
            wrec.processCreatedResponse(wr);
        } else if(cancelled) {
            wrec = activeWatchers.remove(watchId);
            if(wrec != null) {
                //TODO try to resume on "unexpected" cancellations?
                wrec.processCancelledResponse(wr);
            }
        } else {
            wrec = activeWatchers.get(watchId);
            if(wrec != null) {
                wrec.processWatchEvents(wr);
            } else {
                logger.warn("State error: received response for unrecognized watcher: "+watchId);
                //TODO other action TBD
            }
        }
    }
    
    @GuardedBy("eventLoop")
    protected int errCounter = 0;
    
    @GuardedBy("this") // (and eventLoop currently)
    protected boolean waiting = false;
    
    protected void dispatchStreamCompleted(final boolean doErrorProcessing) {
        eventLoop.execute(() -> {
            if(!closed && doErrorProcessing) {
                errCounter++;
                if(errCounter > 1) {
                    synchronized(NewEtcdWatchImpl.this) {
                        waiting = true;
                    }
                    long delay = 500L * (1L << Math.min(errCounter-2, 4));
                    System.out.println("waiting "+delay+"ms..."); 
                    ses.schedule(() -> dispatchStreamCompleted(false),
                            delay, TimeUnit.MILLISECONDS);
                    return;
                }
            }
            streamCompleted();
        });
    }
    
    @GuardedBy("eventLoop")
    protected void streamCompleted() {
        System.out.println("streamComplete starting");
        List<WatcherRecord> pending;
        synchronized(this) {
            waiting = false;
            requestStream = null; // invalidate reqstream, can't be renewed until after this sync block
            pending = new ArrayList<WatcherRecord>(pendingCreate);
            pending.addAll(activeWatchers.values());
            pendingCreate.clear();
            activeWatchers.clear();
        }
        
        // recreate all the non-cancelled watches
        for(WatcherRecord wrec : pending) {
            if(wrec.finished) continue;
            wrec.watchId = -1L;
            boolean cancelled = wrec.userCancelled || closed;
            if(!cancelled) {
                WatchRequest createReq = wrec.newCreateWatchRequest();
                System.out.println("sending resume req: "+createReq);
                synchronized(this) {
                    StreamObserver<WatchRequest> reqStream = getRequestStream();
                    if(reqStream == null) cancelled = true;
                    else {
                        pendingCreate.add(wrec);
                        reqStream.onNext(createReq); //TODO can this throw? if so things would end up in a bad state
                    }
                }
            }
            if(cancelled) {
                wrec.vUserCancelled = wrec.userCancelled = true;
                wrec.finished = true;
                wrec.publishCompletionEvent(null);
            }
        }
    }
    
    
    @GuardedBy("this") // but lazy-read from other contexts
    protected boolean closed;
    
    @Override
    public void close() {
        if(closed) return;
        if(ses != observerExecutor) ses.shutdownNow();
        System.out.println("in close meth");
        eventLoop.execute(() -> {
            if(!closed) synchronized(NewEtcdWatchImpl.this) {
                if(closed) return;
                closed = true;
                if(waiting) {
                    streamCompleted();
                }
                else if(requestStream != null) {
                    System.out.println("calling onError to cancel");
                    requestStream.onError(new CancellationException());
                }

            }
        });
    }
    
    protected static EventType mapEventType(Event.EventType type) {
        switch(type) {
        case DELETE: return EventType.DELETE;
        case PUT: return EventType.PUT;
        case UNRECOGNIZED:
        default: return EventType.UNRECOGNIZED;
        }
    }
    
    protected static WatchRequest optionToWatchCreateRequest(ByteString key,
            WatchOption option, Long startRevision) {
        WatchCreateRequest.Builder builder = WatchCreateRequest.newBuilder()
                .setKey(key).setPrevKv(option.isPrevKV())
                .setProgressNotify(option.isProgressNotify()) //TODO probably hardcode true
                .setStartRevision(startRevision == null ?
                        option.getRevision() : startRevision);
        if (option.getEndKey().isPresent()) builder.setRangeEnd(option.getEndKey().get());
        if (option.isNoDelete()) builder.addFilters(WatchCreateRequest.FilterType.NODELETE);
        if (option.isNoPut()) builder.addFilters(WatchCreateRequest.FilterType.NOPUT);
        return WatchRequest.newBuilder().setCreateRequest(builder).build();
    }

}
