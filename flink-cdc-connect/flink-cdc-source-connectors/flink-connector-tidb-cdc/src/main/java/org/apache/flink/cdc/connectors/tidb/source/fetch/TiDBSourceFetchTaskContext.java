package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;

public class TiDBSourceFetchTaskContext extends JdbcSourceFetchTaskContext {
  private final TiDBConnection connection;

  public TiDBSourceFetchTaskContext(
      JdbcSourceConfig sourceConfig,
      JdbcDataSourceDialect dataSourceDialect,
      TiDBConnection connection) {
    super(sourceConfig, dataSourceDialect);
    this.connection = connection;
  }

  @Override
  public void configure(SourceSplitBase sourceSplitBase) {
    final TiDBConnectorConfig connectorConfig = getDbzConnectorConfig();
    final boolean tableIdCaseInsensitive =
        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
    TopicSelector<TableId> topicSelector =
        TopicSelector.defaultSelector(
            connectorConfig,
            (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.identifier()));
  }

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

  @Override
  public TiDBConnectorConfig getDbzConnectorConfig() {
    return (TiDBConnectorConfig) super.getDbzConnectorConfig();
  }
}
