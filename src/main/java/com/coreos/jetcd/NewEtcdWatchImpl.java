package com.coreos.jetcd;

import java.io.Closeable;
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

import com.coreos.jetcd.api.Event;
import com.coreos.jetcd.api.KeyValue;
import com.coreos.jetcd.api.ResponseHeader;
import com.coreos.jetcd.api.WatchCancelRequest;
import com.coreos.jetcd.api.WatchCreateRequest;
import com.coreos.jetcd.api.WatchGrpc;
import com.coreos.jetcd.api.WatchRequest;
import com.coreos.jetcd.api.WatchResponse;
import com.coreos.jetcd.data.EtcdHeader;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchCreateException;
import com.coreos.jetcd.watch.WatchEvent;
import com.coreos.jetcd.watch.WatchEvent.EventType;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

/**
 * 
 * @author nickhill
 */
public class NewEtcdWatchImpl implements EtcdWatch, AutoCloseable {
    
    /* 
     * Internal rework in progress! Should be functional though
     */
    
    /* Watcher states:
     *   - in pendingCreate only, etcdWatchId == -1, finished == false
     *   - in activeWatchers only, etcWatchId >= 0, finished == false
     *          (stream completion event published in this transition)
     *   - in neither, etcdWatchId == -1, finished = true
     */
    
    private final WatchGrpc.WatchStub watchStub;
    private final Executor observerExecutor;
    private final ScheduledExecutorService ses;
    
    @GuardedBy("this")
    private StreamObserver<WatchRequest> requestStream;
    private final Executor eventLoop;
    
    private final Queue<WatcherRecord> pendingCreate = new ConcurrentLinkedQueue<>();
    
    // this doesn't need to be synchronized since it's only
    // accessed by the response stream observer methods
    private final Map<Long,WatcherRecord> activeWatchers = new HashMap<>();
    
    
    
    public NewEtcdWatchImpl(ManagedChannel channel, Optional<String> token) {
        this(channel, token, null);
    }
    
    public NewEtcdWatchImpl(ManagedChannel channel, Optional<String> token, Executor executor) {
        this.watchStub = EtcdClientUtil.configureStub(WatchGrpc.newStub(channel), token);
        if(executor == null) executor = ForkJoinPool.commonPool();
        this.observerExecutor = executor;
        
        //TODO use same as grpc one
        this.eventLoop = new SerializingExecutor(executor);
        
        this.ses = executor instanceof ScheduledExecutorService
                ? (ScheduledExecutorService)executor
                        : Executors.newSingleThreadScheduledExecutor();
    }
    
    
    
    
    class WatcherRecord {
        
        private final StreamObserver<WatchEvent> observer;
        private final ByteString key;
        private final WatchOption options;
        private final Executor watcherExecutor;
        
        long upToRevision;
        long etcdWatchId = -1L;
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
            this.watcherExecutor = new SerializingExecutor(parentExecutor);
        }
        
        /**
         * at most one param non-null; both params null => complete
         */
        public void publishWatchEvent(final WatchEvent watchEvent, final Throwable t) {
            watcherExecutor.execute(() -> {
                try {
                    if(watchEvent != null) {
                        if(!vUserCancelled) observer.onNext(watchEvent);
                    }
                    else if(t != null && !vUserCancelled) observer.onError(t);
                    else observer.onCompleted();
                } catch(Exception e) {
                    //TODO log "observer method threw", e
                    if(watchEvent != null) {
                        // must also cancel the watch here per StreamObserver contract
                        cancelWatcher(WatcherRecord.this);
                        //TODO this will result in the watcher receiving
                        // a final onComplete, but it should really be onError in this case
                    }
                }
            });
        }
        
        public void processWatchEvents(WatchResponse wr) {
            if(userCancelled) return; // suppress events if cancelled
            if(wr.getEventsCount() > 0) {
                //TODO optimize this - submit list of events instead, with a min-revision
                
                EtcdHeader etcdHeader = new EtcdHeader(wr.getHeader(), wr.getCompactRevision());
                for(Event event : wr.getEventsList()) {
                    KeyValue kv = event.getKv();
                    boolean skip = false;
                    if(kv != null) {
                        if(kv.getModRevision() <= upToRevision) skip = true;
                        else upToRevision = kv.getModRevision();
                    }
                    if(!skip) {
                        WatchEvent watchEvent = new WatchEvent(etcdHeader,
                                event.getKv(), event.getPrevKv(),
                                mapEventType(event.getType()));
                        publishWatchEvent(watchEvent, null);
                    }
                }
            }
            upToRevision = wr.getHeader().getRevision(); //TODO make sure this rev is appropriate
        }
        
        public void processCancelledEvent(ResponseHeader responseHeader, long compactRevision) {
            etcdWatchId = -1L;
            if(finished) {
                //TODO unexpected - log
            } else {
                finished = true;
                if(userCancelled) publishWatchEvent(null, null);
                else {
                    EtcdHeader etcdHeader = new EtcdHeader(responseHeader, compactRevision);
                    Throwable err = new WatchCreateException("TODO", etcdHeader);
                    publishWatchEvent(null, err);
                }
            }
        }
        
        public WatchRequest newCreateWatchRequest() {
            return optionToWatchCreateRequest(key, options, upToRevision +1);
        }
    }
    
    @Override
    public Closeable watch(ByteString key, WatchOption watchOption, StreamObserver<WatchEvent> observer) {
        Preconditions.checkNotNull(key, "key can't be null");
        Preconditions.checkNotNull(key, "observer can't be null");
        if(watchOption == null) watchOption = WatchOption.DEFAULT;
        if(closed) throw new IllegalStateException("closed");
        final WatcherRecord wrec = new WatcherRecord(key, watchOption, observer, observerExecutor);
        WatchRequest createReq = wrec.newCreateWatchRequest();
        synchronized(this) {
            StreamObserver<WatchRequest> reqStream = getRequestStream();
            if(reqStream == null) throw new IllegalStateException("closed");
            pendingCreate.add(wrec);
            reqStream.onNext(createReq); //TODO can this throw? if so things would end up in a bad state
            System.out.println("sent new create request");
        }
        return new Closeable() {
            @Override
            public void close() {
                cancelWatcher(wrec);
            }
        };
    }

    protected void cancelWatcher(WatcherRecord wrec) {
        if(closed || wrec == null || wrec.finished || wrec.userCancelled) return;
        eventLoop.execute(() -> {
                if(closed || wrec.userCancelled || wrec.finished) return;
                sendCancel(wrec);
                wrec.vUserCancelled = wrec.userCancelled = true;
        });
    }
    
    protected void sendCancel(WatcherRecord wrec) {
        if(waiting) {
            wrec.finished = true;
            wrec.publishWatchEvent(null, null);
            return;
        }
        
        if(wrec.etcdWatchId < 0) return;
        // send cancel request
        WatchRequest cancelReq = WatchRequest.newBuilder()
                .setCancelRequest(WatchCancelRequest.newBuilder()
                        .setWatchId(wrec.etcdWatchId).build()).build();
        synchronized(this) {
            StreamObserver<WatchRequest> reqStream = getRequestStream();
            if(reqStream != null) {
                reqStream.onNext(cancelReq);
                System.out.println("sent new cancel request");
            }
        }
    }
    
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
            //TODO log here
            dispatchStreamCompleted(true);
        }
    };

    protected void processResponse(WatchResponse wr) {
        boolean created = wr.getCreated();
        boolean cancelled = wr.getCanceled();
        boolean tooOld = wr.getCompactRevision() != 0;
        long etcdId = wr.getWatchId();
        WatcherRecord wrec;
        if(created) {
            wrec = pendingCreate.poll();
            if(wrec == null) {
                //TODO state err - probably log, cancel and forget; *maybe* close stream and refresh all
                throw new IllegalStateException();
            } else {
                if(cancelled || tooOld || etcdId == -1L) { //TODO probably distinguish in err message
                    wrec.processCancelledEvent(wr.getHeader(), wr.getCompactRevision());
                } else {
                    wrec.etcdWatchId = etcdId;
                    if(activeWatchers.putIfAbsent(etcdId, wrec) == null) {
                        if(wrec.userCancelled) sendCancel(wrec);
                        else {
                            wrec.processWatchEvents(wr);

                            //TODO publish "caught up" event now?
                            //  based on seen revision being caught up to startrevision
                            // OR "current" rev being earlier than startrevision
                            // (may not have sent other events)
                        }
                    } else {
                        //TODO watch_id conflict (should not happen)
                         // - log, other action TBD (probably cancel existing one)
                    }
                }
            }
        } else if(cancelled || tooOld) {
            wrec = activeWatchers.remove(etcdId);
            if(wrec != null) {
                //TODO try to resume on "unexpected" cancellations?
                wrec.processCancelledEvent(wr.getHeader(), wr.getCompactRevision());
            }
        } else {
            wrec = activeWatchers.get(etcdId);
            if(wrec != null) {
                wrec.processWatchEvents(wr);
            } else {
                //TODO state error - log, other action TBD
            }
        }
    }
    
    int errCounter = 0;
    boolean waiting = false;
    
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
    
    // only called from StreamObserver methods
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
            wrec.etcdWatchId = -1L;
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
                wrec.publishWatchEvent(null, null);
            }
        }
    }
    
    
    @GuardedBy("this")
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
