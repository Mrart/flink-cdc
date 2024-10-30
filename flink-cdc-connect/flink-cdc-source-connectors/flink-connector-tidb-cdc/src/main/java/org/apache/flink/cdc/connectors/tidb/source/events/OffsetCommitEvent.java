package org.apache.flink.cdc.connectors.tidb.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

public class OffsetCommitEvent implements SourceEvent {
  private static final long serialVersionUID = 1L;

  private final boolean commitOffset;

  public OffsetCommitEvent(boolean commitOffset) {
    this.commitOffset = commitOffset;
  }

  public boolean isCommitOffset() {
    return commitOffset;
  }
}
