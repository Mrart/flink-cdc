package org.apache.flink.cdc.connectors.tidb.source.schema;

import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.connector.tidb.TidbTopicSelector;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

public class TiDBSchema {
    private static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE ";
    private static final String DESC_TABLE = "DESC ";


    private final TiDBConnectorConfig connectorConfig;
    private final TiDBDatabaseSchema databaseSchema;
    private final Map<TableId, TableChange> schemasByTableId;

    //    public TiDBSchema(Map<TableId, TableChanges.TableChange> schemasByTableId) {
    //        this.schemasByTableId = schemasByTableId;
    //    }


    public TiDBSchema(TiDBSourceConfig sourceConfig, boolean isTableIdCaseSensitive) {
        this.connectorConfig = sourceConfig.getDbzConnectorConfig();
        this.databaseSchema = createTiDBDatabaseSchema(connectorConfig, isTableIdCaseSensitive);
        this.schemasByTableId = new HashMap<>();
    }

    public TableChange getTableSchema(JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = readTableSchema(jdbc, tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    public static TiDBDatabaseSchema createTiDBDatabaseSchema(
            TiDBConnectorConfig dbzTiDBConfig, boolean isTableIdCaseSensitive) {
        TopicSelector<TableId> topicSelector = TidbTopicSelector.defaultSelector(dbzTiDBConfig);
//        Key.KeyMapper customKeysMapper = new CustomeKeyMapper();
        return new TiDBDatabaseSchema(
                dbzTiDBConfig,
                topicSelector,
                isTableIdCaseSensitive,
                dbzTiDBConfig.getKeyMapper());
    }

    private TableChange readTableSchema(JdbcConnection jdbc, TableId tableId){
//        final Map<TableId, TableChanges.TableChange> tableChangeMap = new HashMap<>();
        MySqlOffsetContext offsetContext = MySqlOffsetContext.initial(connectorConfig);
        final TiDBPartition partition =
                new TiDBPartition(connectorConfig.getLogicalName());
//        final String sql = "SHOW CREATE TABLE " + TiDBUtils.quote(tableId);

        offsetContext.event(tableId, Instant.now());
        Tables tables = new Tables();

        try {
            jdbc.readSchema(
                    tables,
                    connectorConfig.databaseName(),
                    tableId.schema(),
                    connectorConfig.getTableFilters().dataCollectionFilter(),
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
                        connectorConfig.databaseName(),
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
