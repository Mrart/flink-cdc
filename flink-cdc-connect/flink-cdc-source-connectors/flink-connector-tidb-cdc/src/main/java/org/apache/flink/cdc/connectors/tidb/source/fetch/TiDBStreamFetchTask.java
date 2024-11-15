package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBStreamFetchTask implements FetchTask<SourceSplitBase> {
  private static final Logger LOG = LoggerFactory.getLogger(TiDBStreamFetchTask.class);
  private final StreamSplit split;
  private volatile boolean taskRunning = false;

  public TiDBStreamFetchTask(StreamSplit split) {
    this.split = split;
  }

  @Override
  public void execute(Context context) throws Exception {}

  @Override
  public boolean isRunning() {
    return taskRunning;
  }

  @Override
  public SourceSplitBase getSplit() {
    return split;
  }

  @Override
  public void close() {
    taskRunning = false;
  }
}