package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBScanFetchTask extends AbstractScanFetchTask {
  private static final Logger LOG = LoggerFactory.getLogger(TiDBScanFetchTask.class);

  public TiDBScanFetchTask(SnapshotSplit snapshotSplit) {
    super(snapshotSplit);
  }

  @Override
  protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
      throws Exception {}

  @Override
  protected void executeDataSnapshot(Context context) throws Exception {}
}
