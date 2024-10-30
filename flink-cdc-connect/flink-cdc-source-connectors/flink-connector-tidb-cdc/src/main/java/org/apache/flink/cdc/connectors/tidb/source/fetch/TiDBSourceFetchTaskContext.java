package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;

public class TiDBSourceFetchTaskContext extends JdbcSourceFetchTaskContext {
  public TiDBSourceFetchTaskContext(
      JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dataSourceDialect) {
    super(sourceConfig, dataSourceDialect);
  }

  @Override
  public void configure(SourceSplitBase sourceSplitBase) {}

  @Override
  public ChangeEventQueue<DataChangeEvent> getQueue() {
    return null;
  }

  @Override
  public Tables.TableFilter getTableFilter() {
    return null;
  }

  @Override
  public Offset getStreamOffset(SourceRecord record) {
    return null;
  }

  @Override
  public void close() throws Exception {}

  @Override
  public RelationalDatabaseSchema getDatabaseSchema() {
    return null;
  }

  @Override
  public RowType getSplitType(Table table) {
    return null;
  }

  @Override
  public ErrorHandler getErrorHandler() {
    return null;
  }

  @Override
  public JdbcSourceEventDispatcher getDispatcher() {
    return null;
  }

  @Override
  public OffsetContext getOffsetContext() {
    return null;
  }

  @Override
  public Partition getPartition() {
    return null;
  }
}
