package com.coreos.jetcd;

import java.io.Closeable;

import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

/**
 * Interface of watch client
 */
public interface EtcdWatch  extends AutoCloseable {


  /**
   * Watch watches on a key or prefix. The watched events will be called by onWatch.
   * If the watch is slow or the required rev is compacted, the watch request
   * might be canceled from the server-side and the onError will be called.
   *
   * @param key         the key subscribe
   * @param watchOption key option
   * @param events    event stream
   * @return Closeable close to cancel
   */
    Closeable watch(ByteString key, WatchOption watchOption, StreamObserver<WatchEvent> events);
  
  @Override
  default void close() {

  }
  
}