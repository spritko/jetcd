package com.coreos.jetcd.watch;


import com.coreos.jetcd.api.ResponseHeader;

/**
 * Exception thrown when create watcher failed.
 */
public class RevisionCompactedException extends Exception {

  public final ResponseHeader header;
  public final long compactRevision;

  public RevisionCompactedException(ResponseHeader header, long compactRevision) {
    super("Watch revision has been compacted, oldest available is "+compactRevision);
    this.header = header;
    this.compactRevision = compactRevision;
  }
}
