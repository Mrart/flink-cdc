package org.apache.flink.cdc.connectors.tidb.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupMode;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceRecordEmitter;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBSchemaUtils;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import io.debezium.relational.TableId;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkEvent.isLowWatermarkEvent;

/** The {@link RecordEmitter} implementation for pipeline oracle connector. */
public class TiDBPipelineRecordEmitter<T> extends IncrementalSourceRecordEmitter<T> {
    private final TiDBSourceConfig tiDBSourceConfig;
    private final TiDBDialect tiDBDialect;

    // Used when startup mode is initial
    private Set<TableId> alreadySendCreateTableTables;

    // Used when startup mode is not initial
    private boolean alreadySendCreateTableForBinlogSplit = false;
    private final List<CreateTableEvent> createTableEventCache;

    public TiDBPipelineRecordEmitter(
            DebeziumDeserializationSchema<T> debeziumDeserializationSchema,
            SourceReaderMetrics sourceReaderMetrics,
            TiDBSourceConfig tiDBSourceConfig,
            OffsetFactory offsetFactory,
            TiDBDialect tiDBDialect) {
        super(
                debeziumDeserializationSchema,
                sourceReaderMetrics,
                tiDBSourceConfig.isIncludeSchemaChanges(),
                offsetFactory);

        this.tiDBSourceConfig = tiDBSourceConfig;
        this.tiDBDialect = tiDBDialect;
        this.alreadySendCreateTableTables = new HashSet<>();
        this.createTableEventCache = new ArrayList<>();

        if (!tiDBSourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
            try (TiDBConnection jdbc = tiDBDialect.openJdbcConnection()) {
                List<TableId> capturedTableIds =
                        TableDiscoveryUtils.listTables(
                                tiDBSourceConfig.getDatabaseList().get(0),
                                jdbc,
                                tiDBSourceConfig.getTableFilters());
                for (TableId tableId : capturedTableIds) {
                    Schema tableSchema = TiDBSchemaUtils.getTableSchema(tableId,tiDBSourceConfig,jdbc);
                    createTableEventCache.add(
                            new CreateTableEvent(
                                    org.apache.flink.cdc.common.event.TableId.tableId(
                                            tableId.schema(), tableId.table()),
                                    tableSchema));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void processElement(
            SourceRecord element, SourceOutput<T> output, SourceSplitState splitState)
            throws Exception {
        if (isLowWatermarkEvent(element) && splitState.isSnapshotSplitState()) {
            TableId tableId = splitState.asSnapshotSplitState().toSourceSplit().getTableId();
            if (!alreadySendCreateTableTables.contains(tableId)) {
                try (TiDBConnection jdbc = tiDBDialect.openJdbcConnection()) {
                    sendCreateTableEvent(jdbc, tableId, (SourceOutput<Event>) output);
                    alreadySendCreateTableTables.add(tableId);
                }
            }
        } else if (splitState.isStreamSplitState()
                && !alreadySendCreateTableForBinlogSplit
                && !tiDBSourceConfig.getStartupOptions().startupMode.equals(StartupMode.INITIAL)) {
            for (CreateTableEvent createTableEvent : createTableEventCache) {
                output.collect((T) createTableEvent);
            }
            alreadySendCreateTableForBinlogSplit = true;
        }
        super.processElement(element, output, splitState);
    }

    private void sendCreateTableEvent(
            TiDBConnection jdbc, TableId tableId, SourceOutput<Event> output) {
        Schema schema = TiDBSchemaUtils.getTableSchema(tableId, tiDBSourceConfig, jdbc);
        output.collect(
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                tableId.schema(), tableId.table()),
                        schema));
    }
}
