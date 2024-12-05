package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;

import java.util.Map;

import static org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset.NO_STOPPING_OFFSET;

public class CDCEventOffsetFactory extends OffsetFactory {

  @Override
  public Offset newOffset(Map<String, String> offset) {
    return new CDCEventOffset(offset);
  }

  @Override
  public Offset newOffset(String filename, Long position) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Offset newOffset(Long position) {
    return new CDCEventOffset(position);
  }

  @Override
  public Offset createTimestampOffset(long timestampMillis) {
    return new CDCEventOffset(timestampMillis);
  }

  @Override
  public Offset createInitialOffset() {
    return CDCEventOffset.INITIAL_OFFSET;
  }

  @Override
  public Offset createNoStoppingOffset() {
    return NO_STOPPING_OFFSET;
  }
}
