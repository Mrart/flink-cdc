package org.apache.flink.cdc.connectors.tidb.source.schema;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class TiDBSchema {
    private final Map<TableId, TableChanges.TableChange> schemasByTableId = new HashMap<>();
    private final JdbcConnection jdbcConnection;
    private final TiDBConnectorConfig dbzConfig;

//    public TiDBSchema(Map<TableId, TableChanges.TableChange> schemasByTableId) {
//        this.schemasByTableId = schemasByTableId;
//    }


    public TiDBSchema(JdbcConnection jdbc, TiDBSourceConfig sourceConfig) {
        this.jdbcConnection = jdbc;
        this.dbzConfig = sourceConfig.getDbzConnectorConfig();

    }

    public TableChanges.TableChange getTableSchema(TableId tableId) {
        // read schema from cache first
        if (!schemasByTableId.containsKey(tableId)) {
            try {
                readTableSchema(tableId);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to read table schema", e);
            }
        }
        return schemasByTableId.get(tableId);
    }

    private TableChanges.TableChange readTableSchema(TableId tableId) throws SQLException {

        final CDCEventOffsetContext offsetContext =
                CDCEventOffsetContext.initialContext(dbzConfig, jdbcConnection, Clock.SYSTEM);

        TiDBPartition partition = new TiDBPartition(dbzConfig.getLogicalName());
        // set the events to populate proper sourceInfo into offsetContext
        offsetContext.event(tableId, Instant.now());

        Tables tables = new Tables();
        try {
            jdbcConnection.readSchema(
                    tables,
                    dbzConfig.databaseName(),
                    tableId.schema(),
                    dbzConfig.getTableFilters().dataCollectionFilter(),
                    null,
                    false);
        } catch (SQLException e) {
            throw new FlinkRuntimeException("Failed to read schema", e);
        }

        Table table = Objects.requireNonNull(tables.forTable(tableId));

        // TODO: check whether we always set isFromSnapshot = true
        SchemaChangeEvent schemaChangeEvent =
                SchemaChangeEvent.ofCreate(
                        partition,
                        offsetContext,
                        dbzConfig.databaseName(),
                        tableId.schema(),
                        null,
                        table,
                        true);

        for (TableChanges.TableChange tableChange : schemaChangeEvent.getTableChanges()) {
            this.schemasByTableId.put(tableId, tableChange);
        }
        return this.schemasByTableId.get(tableId);
    }
}
