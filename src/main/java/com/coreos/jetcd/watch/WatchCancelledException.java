package com.coreos.jetcd.watch;

import com.coreos.jetcd.api.ResponseHeader;

/**
 * Exception thrown when create watcher failed.
 */
public class WatchCancelledException extends Exception {

  public final ResponseHeader header;

  public WatchCancelledException(ResponseHeader header) {
    super("Watch was cancelled by the server unexpectedly");
    this.header = header;
  }
}
