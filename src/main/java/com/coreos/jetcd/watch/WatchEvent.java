package com.coreos.jetcd.watch;

import com.coreos.jetcd.api.KeyValue;
import com.coreos.jetcd.data.EtcdHeader;
import com.google.protobuf.ByteString;

/**
 * Watch event, return by watch, contain put, delete event.
 */
public class WatchEvent {

  public enum EventType {
    PUT,
    DELETE,
    UNRECOGNIZED
    // maybe CAUGHT_UP
    // maybe RECONNECTING
  }
  
  private final EventType eventType;
  
  private final EtcdHeader etcdHeader;

  private final KeyValue keyValue;

  private final KeyValue prevKV;

  public WatchEvent(EtcdHeader etcdHeader,
          KeyValue keyValue, KeyValue prevKV, EventType eventType) {
    this.etcdHeader = etcdHeader;
    this.keyValue = keyValue;
    this.prevKV = prevKV;
    this.eventType = eventType;
  }
  
  public EtcdHeader getEtcdHeader() {
    return etcdHeader;
  }

  public KeyValue getKeyValue() {
    return keyValue;
  }

  public KeyValue getPrevKV() {
    return prevKV;
  }

  public EventType getEventType() {
    return eventType;
  }
  
  @Override
    public String toString() {
        return "WatchEvent[type="+eventType
                +",key="+kvToString(keyValue)
                +",modRev="+(keyValue!=null?keyValue.getModRevision():"n/a")
                +",prevKey="+kvToString(prevKV)+"]";
    }
  
  private static String kvToString(KeyValue kv) {
      if(kv == null) return null;
      ByteString key = kv.getKey();
      return key != null ? key.toStringUtf8() : "null";
  }
}
