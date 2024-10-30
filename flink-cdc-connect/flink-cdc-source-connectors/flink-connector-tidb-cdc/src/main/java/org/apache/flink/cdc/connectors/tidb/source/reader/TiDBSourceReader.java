package org.apache.flink.cdc.connectors.tidb.source.reader;

import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.DataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderWithCommit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class TiDBSourceReader extends IncrementalSourceReaderWithCommit {
  private static final Logger LOG = LoggerFactory.getLogger(TiDBSourceReader.class);

  public TiDBSourceReader(
      FutureCompletingBlockingQueue elementQueue,
      Supplier supplier,
      RecordEmitter recordEmitter,
      Configuration config,
      IncrementalSourceReaderContext incrementalSourceReaderContext,
      SourceConfig sourceConfig,
      SourceSplitSerializer sourceSplitSerializer,
      DataSourceDialect dialect) {
    super(
        elementQueue,
        supplier,
        recordEmitter,
        config,
        incrementalSourceReaderContext,
        sourceConfig,
        sourceSplitSerializer,
        dialect);
  }
}
