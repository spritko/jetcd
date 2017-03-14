package com.coreos.jetcd;


import com.coreos.jetcd.api.*;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchCreateException;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

import static com.coreos.jetcd.EtcdUtil.apiToClientEvents;
import static com.coreos.jetcd.EtcdUtil.apiToClientHeader;

/**
 * etcd watcher Implementation
 */
public class EtcdWatchImpl implements EtcdWatch {

  private static final Logger logger = LoggerFactory.getLogger(EtcdWatchImpl.class);

  private volatile StreamObserver<WatchRequest> requestStream;

  private ConcurrentHashMap<Long, WatcherImpl> watchers = new ConcurrentHashMap<>();

  private final WatchGrpc.WatchStub watchStub;

  private ConcurrentLinkedQueue<Pair<WatcherImpl, CompletableFuture<Watcher>>> pendingCreateWatchers = new ConcurrentLinkedQueue<>();
  private Map<Long, CompletableFuture<Boolean>> pendingCancelFutures = new ConcurrentHashMap<>();

  public EtcdWatchImpl(ManagedChannel channel, Optional<String> token) {
    this.watchStub = EtcdClientUtil.configureStub(WatchGrpc.newStub(channel), token);
  }

  /**
   * Watch watches on a key or prefix. The watched events will be called by onWatch.
   * If the watch is slow or the required rev is compacted, the watch request
   * might be canceled from the server-side and the onCreateFailed will be called.
   *
   * @param key         the key subscribe
   * @param watchOption key option
   * @param callback    call back
   * @return CompletableFuture watcher
   */
  @Override
  public CompletableFuture<Watcher> watch(ByteSequence key, WatchOption watchOption, WatchCallback callback) {
    WatchRequest request = optionToWatchCreateRequest(EtcdUtil.byteStringFromByteSequence(key), watchOption);
    WatcherImpl watcher = new WatcherImpl(key, watchOption, callback);
    CompletableFuture<Watcher> waitFuture = new CompletableFuture();
    synchronized (this) {
      this.pendingCreateWatchers.add(new Pair<>(watcher, waitFuture));
      getRequestStream().onNext(request);
    }
    return waitFuture;
  }

  /**
   * Cancel the watch task with the watcher, the onCanceled will be called after successfully canceled.
   *
   * @param id the watcher to be canceled
   */
  protected CompletableFuture<Boolean> cancelWatch(long id) {
    WatcherImpl temp = watchers.get(id);
    CompletableFuture<Boolean> completableFuture = null;
    if (temp != null) {
      synchronized (temp) {
        if (this.watchers.containsKey(temp.getWatchID())) {
          this.watchers.remove(temp.getWatchID());
          completableFuture = new CompletableFuture<>();
          this.pendingCancelFutures.put(id, completableFuture);
        }
      }
    }

    WatchCancelRequest cancelRequest = WatchCancelRequest.newBuilder().setWatchId(id).build();
    WatchRequest request = WatchRequest.newBuilder().setCancelRequest(cancelRequest).build();
    getRequestStream().onNext(request);
    return completableFuture;
  }

  /**
   * empty the old request stream, watchers and resume the old watchers
   * empty the pendingCancelFutures as there is no need to cancel, the old request stream has been dead
   */
  private synchronized void resume() {
    this.requestStream = null;
    WatcherImpl[] resumeWatchers = watchers.values().toArray( new WatcherImpl[watchers.size()] );
    this.watchers.clear();
    for (CompletableFuture<Boolean> watcherCompletableFuture : pendingCancelFutures.values()) {
      watcherCompletableFuture.complete(Boolean.TRUE);
    }
    this.pendingCancelFutures.clear();
    resumeWatchers(resumeWatchers);
  }

  /**
   * single instance method to get request stream, empty the old requestStream, so we will get a new
   * requestStream automatically
   * <p>the responseStream will distribute the create, cancel, normal response to
   * processCreate, processCanceled and processEvents
   * <p>if error happened, the requestStream will be closed by server side, so we call resume to resume
   * all ongoing watchers
   *
   * @return
   */
  private StreamObserver<WatchRequest> getRequestStream() {
    if (this.requestStream == null) {
      synchronized (this) {
        if (this.requestStream == null) {
          if ( logger.isDebugEnabled()) {
            logger.debug("Creating new requestStream");
          }
          StreamObserver<WatchResponse> watchResponseStreamObserver = new StreamObserver<WatchResponse>() {
            @Override
            public void onNext(WatchResponse watchResponse) {
              if ( logger.isDebugEnabled() ) {
                logger.debug("onNext watchId:" + watchResponse.getWatchId()
                        + "  events:" + watchResponse.getEventsList()
                        + " create:" + watchResponse.getCreated()
                        + " cancelled:" + watchResponse.getCanceled());
              }
              if (watchResponse.getCreated()) {
                processCreate(watchResponse);
              } else if (watchResponse.getCanceled()) {
                processCanceled(watchResponse);
              } else {
                processEvents(watchResponse);
              }
            }

            @Override
            public void onError(Throwable throwable) {
              if ( logger.isDebugEnabled() ) {
                logger.debug("onError " + throwable);
              }
              resume();
            }

            @Override
            public void onCompleted() {
              if ( logger.isDebugEnabled() ) {
                logger.debug("onCompleted ");
              }

            }
          };
          this.requestStream = this.watchStub.watch(watchResponseStreamObserver);
        }
      }
    }
    return this.requestStream;
  }

  /**
   * Process create response from etcd server
   * <p>If there is no pendingWatcher, ignore.
   * <p>If cancel flag is true or CompactRevision not equal zero means the start revision
   * has been compacted out of the store, call onCreateFailed.
   * <p>If watchID = -1, complete future with WatchCreateException.
   * <p>If everything is Ok, create watcher, complete CompletableFuture task and put the new watcher
   * to the watchers map.
   *
   * @param response
   */
  private void processCreate(WatchResponse response) {
    Pair<WatcherImpl, CompletableFuture<Watcher>> requestPair = pendingCreateWatchers.poll();
    WatcherImpl watcher = requestPair.getKey();
    if (response.getCreated()) {
      if (response.getCanceled() || response.getCompactRevision() != 0) {
        watcher.setCanceled(true);
        requestPair.getValue().completeExceptionally(new WatchCreateException("the start revision has been compacted", apiToClientHeader(response.getHeader(), response.getCompactRevision())));
        ;
      }

      if (response.getWatchId() == -1 && watcher.callback != null) {
        requestPair.getValue().completeExceptionally(new WatchCreateException("create watcher failed", apiToClientHeader(response.getHeader(), response.getCompactRevision())));
      } else {
        if ( logger.isDebugEnabled() ) {
          logger.debug("created watcher  ID:" + response.getWatchId() + " watcher" + watcher.getKey().toStringUtf8());
        }
        this.watchers.put(response.getWatchId(), watcher);
        watcher.setWatchID(response.getWatchId());
        requestPair.getValue().complete(watcher);
      }

      //note the header revision so that put following a current watcher disconnect will arrive
      //on watcher channel after reconnect
      synchronized (watcher) {
        watcher.setLastRevision(response.getHeader().getRevision());
        if (watcher.isResuming()) {
          watcher.setResuming(false);
        }
      }
    }
  }

  /**
   * Process subscribe watch events
   * <p>If the watch id is not in the watchers map, scan it in the pendingCancelFutures map
   * if exist, ignore, otherwise cancel it.
   * <p>If the watcher exist, call the onWatch and set the last revision for resume
   *
   * @param watchResponse
   */
  private void processEvents(WatchResponse watchResponse) {
    WatcherImpl watcher = watchers.get(watchResponse.getWatchId());
    if (watcher != null) {
      synchronized (watcher) {
        if (watchResponse.getEventsCount() != 0) {
          List<Event> events = watchResponse.getEventsList();
          // if on resume process, filter processed events
          if (watcher.isResuming()) {
            long lastRevision = watcher.getLastRevision();
            events.removeIf((e) -> e.getKv().getModRevision() <= lastRevision);
          }
          watcher.setLastRevision(
                  watchResponse
                          .getEvents(watchResponse.getEventsCount() - 1)
                          .getKv().getModRevision());
          if ( logger.isDebugEnabled() ) {
            logger.debug("processEvents WatcherID:" + watchResponse.getWatchId() + " Watcher: " + watcher + " callback:" + watcher.callback);
            logger.debug("    Key " + watchResponse.getEvents(0).getKv().getKey().toStringUtf8() );
          }
          if (watcher.callback != null) {
            watcher.callback.onWatch(apiToClientHeader(watchResponse.getHeader(), watchResponse.getCompactRevision()), apiToClientEvents(events));
          }
        } else {
          watcher.setLastRevision(watchResponse.getHeader().getRevision());
        }
      }
    } else {
      // if the watcher is not canceling, cancel it.
      CompletableFuture<Boolean> completableFuture = this.pendingCancelFutures.get(watchResponse.getWatchId());
      if (this.pendingCancelFutures.putIfAbsent(watcher.getWatchID(), completableFuture) == null) {
        cancelWatch(watchResponse.getWatchId());
      }
    }
  }

  /**
   * resume all the watchers
   *
   * @param watchers
   */
  private void resumeWatchers(WatcherImpl[] watchers) {
    for (WatcherImpl watcher : watchers) {
      if (watcher.callback != null) {
        watcher.callback.onResuming();
      }
      watch(watcher.getKey(), getResumeWatchOptionWithWatcher(watcher), watcher.callback);
    }
  }

  /**
   * Process cancel response from etcd server,
   *
   * @param response
   */
  private void processCanceled(WatchResponse response) {
    if ( logger.isDebugEnabled() ) {
      logger.debug("Cancelled watch " + response.getWatchId() );
    }
    CompletableFuture<Boolean> cancelFuture = this.pendingCancelFutures.remove(response.getWatchId());
    cancelFuture.complete(Boolean.TRUE);
  }

  /**
   * convert WatcherOption to WatchRequest
   *
   * @param key
   * @param option
   * @return
   */
  private WatchRequest optionToWatchCreateRequest(ByteString key, WatchOption option) {
    WatchCreateRequest.Builder builder = WatchCreateRequest.newBuilder()
            .setKey(key)
            .setPrevKv(option.isPrevKV())
            .setProgressNotify(option.isProgressNotify())
            .setStartRevision(option.getRevision());

    if (option.getEndKey().isPresent()) {
      builder.setRangeEnd(EtcdUtil.byteStringFromByteSequence(option.getEndKey().get()));
    }

    if (option.isNoDelete()) {
      builder.addFilters(WatchCreateRequest.FilterType.NODELETE);
    }

    if (option.isNoPut()) {
      builder.addFilters(WatchCreateRequest.FilterType.NOPUT);
    }

    return WatchRequest.newBuilder().setCreateRequest(builder).build();
  }

  /**
   * build new WatchOption from dead to resume it in new requestStream
   *
   * @param watcher
   * @return
   */
  private WatchOption getResumeWatchOptionWithWatcher(Watcher watcher) {
    WatchOption oldOption = watcher.getWatchOption();
    return WatchOption.newBuilder().withNoDelete(oldOption.isNoDelete())
            .withNoPut(oldOption.isNoPut())
            .withPrevKV(oldOption.isPrevKV())
            .withProgressNotify(oldOption.isProgressNotify())
            .withRange(oldOption.getEndKey().orElse(null))
            .withRevision(watcher.getLastRevision() + 1)
            .withResuming(true)
            .build();
  }


  /**
   * Watcher class hold watcher information.
   */
  public class WatcherImpl implements Watcher {

    private final WatchOption watchOption;
    private final ByteSequence key;

    public final WatchCallback callback;
    private long watchID;

    private long lastRevision = -1;
    private boolean canceled = false;

    private boolean resuming;

    private WatcherImpl(ByteSequence key, WatchOption watchOption, WatchCallback callback) {
      this.key = key;
      this.watchOption = watchOption;
      this.callback = callback;
      this.resuming = watchOption.isResuming();
    }

    @Override
    public CompletableFuture<Boolean> cancel() {
      return cancelWatch(watchID);
    }

    /**
     * set the last revision watcher received, used for resume
     *
     * @param lastRevision the last revision
     */
    private void setLastRevision(long lastRevision) {
      this.lastRevision = lastRevision;
    }

    public boolean isCanceled() {
      return canceled;
    }

    private void setCanceled(boolean canceled) {
      this.canceled = canceled;
    }

    /**
     * get the watch id of the watcher
     *
     * @return
     */
    public long getWatchID() {
      return watchID;
    }

    private void setWatchID(long watchID) {
      this.watchID = watchID;
    }

    public WatchOption getWatchOption() {
      return watchOption;
    }

    /**
     * get the last revision watcher received
     *
     * @return last revision
     */
    public long getLastRevision() {
      return lastRevision;
    }

    /**
     * get the watcher key
     *
     * @return watcher key
     */
    public ByteSequence getKey() {
      return key;
    }

    /**
     * whether the watcher is resuming.
     */
    public boolean isResuming() {
      return resuming;
    }

    private void setResuming(boolean resuming) {
      this.resuming = resuming;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
      if (!isCanceled()) {
        try {
          if (!cancel().get(5, TimeUnit.SECONDS)) {

          }
        } catch (InterruptedException e) {
          throw new IOException("Close was interrupted.", e);
        } catch (ExecutionException e) {
          throw new IOException("Exception during execute.", e);
        } catch (TimeoutException e) {
          throw new IOException("Close out of time.", e);
        } finally {
          setCanceled(true);
        }
      }
    }
  }

}
