package com.coreos.jetcd.watch;

import com.coreos.jetcd.api.ResponseHeader;

/**
 * Exception thrown when create watcher failed.
 */
public class WatchCancelledException extends Exception {

  public final ResponseHeader header;

  public WatchCancelledException(String message, ResponseHeader header) {
      super(message);
      this.header = header;
    }
  
  public WatchCancelledException(ResponseHeader header) {
    this("Watch was cancelled by the server unexpectedly", header);
  }
}
