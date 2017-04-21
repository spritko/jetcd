package com.coreos.jetcd.watch;

import com.coreos.jetcd.api.ResponseHeader;

/**
 * Exception thrown when create watcher failed.
 */
public class WatchCreateException extends Exception {

  public final ResponseHeader header;

  public WatchCreateException(ResponseHeader header) {
    super("Watch creation failed");
    this.header = header;
  }
}
