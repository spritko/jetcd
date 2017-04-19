package com.coreos.jetcd.data;

import com.coreos.jetcd.api.ResponseHeader;

/**
 * Etcd message header information.
 */
public class EtcdHeader {
  private final ResponseHeader responseHeader;
  private final long compactRevision;

  public EtcdHeader(ResponseHeader responseHeader, long compactRevision) {
    this.responseHeader = responseHeader;
    this.compactRevision = compactRevision;
  }

  public long getClusterId() {
    return responseHeader.getClusterId();
  }

  public long getMemberId() {
    return responseHeader.getMemberId();
  }

  public long getRevision() {
    return responseHeader.getRevision();
  }

  public long getRaftTerm() {
    return responseHeader.getRaftTerm();
  }

  public long getCompactRevision() {
    return compactRevision;
  }

  @Override
  public String toString() {
    return "EtcdHeader{" +
            "clusterId=" + getClusterId() +
            ", memberId=" + getMemberId() +
            ", revision=" + getRevision() +
            ", raftTerm=" + getRaftTerm() +
            ", compactRevision=" + compactRevision +
            '}';
  }
}
