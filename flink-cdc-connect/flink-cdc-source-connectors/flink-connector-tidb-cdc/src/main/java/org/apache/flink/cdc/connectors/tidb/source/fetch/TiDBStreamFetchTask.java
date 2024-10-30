package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBStreamFetchTask implements FetchTask<SourceSplitBase> {
  private static final Logger LOG = LoggerFactory.getLogger(TiDBStreamFetchTask.class);

  @Override
  public void execute(Context context) throws Exception {}

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public SourceSplitBase getSplit() {
    return null;
  }

  @Override
  public void close() {}
}
