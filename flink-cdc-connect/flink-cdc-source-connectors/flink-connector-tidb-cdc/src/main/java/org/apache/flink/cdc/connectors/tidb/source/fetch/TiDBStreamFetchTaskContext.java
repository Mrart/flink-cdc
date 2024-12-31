package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.handler.TiDBSchemaChangeEventHandler;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetFactory;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetUtils;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.connector.tidb.TidbTaskContext;
import io.debezium.connector.tidb.connection.TiDBEventMetadataProvider;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.TopicSelector;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBStreamFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBSourceFetchTaskContext.class);

    private TidbTaskContext tidbTaskContext;

    private final TiDBConnection connection;
    private TiDBDatabaseSchema tiDBDatabaseSchema;
    private CDCEventOffsetContext offsetContext;
    private SnapshotChangeEventSourceMetrics<TiDBPartition> snapshotChangeEventSourceMetrics;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<TiDBPartition> dispatcher;
    private TiDBPartition tiDBPartition;
    private ChangeEventQueue<DataChangeEvent> queue;
    private ErrorHandler errorHandler;
    private EventMetadataProvider metadataProvider;

    public TiDBStreamFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            TiDBConnection connection) {
        super(sourceConfig, dataSourceDialect);
        this.connection = connection;
        this.metadataProvider = new TiDBEventMetadataProvider();
        this.tiDBDatabaseSchema = tiDBDatabaseSchema;
        this.errorHandler = new ErrorHandler(null, sourceConfig.getDbzConnectorConfig(), queue);
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        final TiDBConnectorConfig connectorConfig = getDbzConnectorConfig();
        final boolean tableIdCaseInsensitive =
                dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
        TopicSelector<TableId> topicSelector =
                TopicSelector.defaultSelector(
                        connectorConfig,
                        (tableId, prefix, delimiter) ->
                                String.join(delimiter, prefix, tableId.identifier()));
        try {
            this.tiDBDatabaseSchema =
                    TiDBUtils.newSchema(
                            connection, connectorConfig, topicSelector, tableIdCaseInsensitive);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize TiDBschema", e);
        }

        this.tiDBPartition = new TiDBPartition(connectorConfig.getLogicalName());
        this.tidbTaskContext = new TidbTaskContext(connectorConfig, tiDBDatabaseSchema);
        this.offsetContext =
                loadStartingOffsetState(
                        new CDCEventOffsetContext.Loader(connectorConfig), sourceSplitBase);
        this.queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(connectorConfig.getPollInterval())
                        .maxBatchSize(connectorConfig.getMaxBatchSize())
                        .maxQueueSize(connectorConfig.getMaxQueueSize())
                        .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                        .loggingContextSupplier(
                                () ->
                                        tidbTaskContext.configureLoggingContext(
                                                "tidb-cdc-connector-task"))
                        .build();

        this.dispatcher =
                new JdbcSourceEventDispatcher<>(
                        connectorConfig,
                        topicSelector,
                        tiDBDatabaseSchema,
                        queue,
                        connectorConfig.getTableFilters().dataCollectionFilter(),
                        DataChangeEvent::new,
                        metadataProvider,
                        schemaNameAdjuster,
                        new TiDBSchemaChangeEventHandler());

        ChangeEventSourceMetricsFactory<TiDBPartition> metricsFactory =
                new DefaultChangeEventSourceMetricsFactory<>();
        this.snapshotChangeEventSourceMetrics =
                metricsFactory.getSnapshotMetrics(tidbTaskContext, queue, metadataProvider);
    }

    public TiDBConnection getConnection() {
        return connection;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return this.sourceConfig.getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return new CDCEventOffset(record.sourceOffset());
    }

    @Override
    public void close() throws Exception {
        this.connection.close();
    }

    @Override
    public TiDBDatabaseSchema getDatabaseSchema() {
        return tiDBDatabaseSchema;
    }

    @Override
    public RowType getSplitType(Table table) {
        return TiDBUtils.getSplitType(table);
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public JdbcSourceEventDispatcher getDispatcher() {
        return dispatcher;
    }

    @Override
    public CDCEventOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public TiDBPartition getPartition() {
        return tiDBPartition;
    }

    @Override
    public TiDBConnectorConfig getDbzConnectorConfig() {
        return (TiDBConnectorConfig) super.getDbzConnectorConfig();
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        CDCEventOffset newOffset = new CDCEventOffset(record.sourceOffset());
        return SourceRecordUtils.splitKeyRangeContains(
                new CDCEventOffset[] {newOffset}, splitStart, splitEnd);
    }

    public SnapshotChangeEventSourceMetrics<TiDBPartition> getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    private CDCEventOffsetContext loadStartingOffsetState(
            CDCEventOffsetContext.Loader loader, SourceSplitBase sourceSplitBase) {
        Offset offset =
                sourceSplitBase.isSnapshotSplit()
                        ? new CDCEventOffsetFactory()
                                .createInitialOffset() // get an offset for starting snapshot
                        : sourceSplitBase.asStreamSplit().getStartingOffset();
        return CDCEventOffsetUtils.getCDCEventOffsetContext(loader, offset);
    }

    public TiDBStreamFetchTaskContext getTaskContext() {
        return this;
    }
}
