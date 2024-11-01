package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlErrorHandler;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.tidb.TiDBOffsetContext;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.connector.tidb.TidbTaskContext;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
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
import org.apache.flink.cdc.connectors.tidb.source.offset.LogMessageOffsetFactory;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

  private static final Logger LOG = LoggerFactory.getLogger(TiDBSourceFetchTaskContext.class);

  private TidbTaskContext tidbTaskContext;

  private final TiDBConnection connection;
  private TiDBDatabaseSchema tiDBDatabaseSchema;
  private TiDBOffsetContext offsetContext;
  private SnapshotChangeEventSourceMetrics<TiDBPartition> snapshotChangeEventSourceMetrics;
  private TopicSelector<TableId> topicSelector;
  private JdbcSourceEventDispatcher<TiDBPartition> dispatcher;
  private TiDBPartition tiDBPartition;
  private ChangeEventQueue<DataChangeEvent> queue;
  private ErrorHandler errorHandler;
  private EventMetadataProvider metadataProvider;



  public TiDBSourceFetchTaskContext(
          JdbcSourceConfig sourceConfig,
          JdbcDataSourceDialect dataSourceDialect,
          TiDBConnection connection) {
    super(sourceConfig, dataSourceDialect);
    this.connection = connection;
    this.tiDBDatabaseSchema = tiDBDatabaseSchema;
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
   this.tiDBDatabaseSchema=TiDBUtils.createTiDBDatabaseSchema(connectorConfig, tableIdCaseInsensitive);
   this.tiDBPartition=new TiDBPartition(connectorConfig.getLogicalName());
   this.tidbTaskContext=new TidbTaskContext(connectorConfig,tiDBDatabaseSchema);
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
