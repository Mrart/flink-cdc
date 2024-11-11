package org.apache.flink.cdc.connectors.tidb.source.schema;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.connector.tidb.TidbTopicSelector;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Key;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.relational.history.TableChanges.TableChange;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.utils.CustomeKeyMapper;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
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
        Key.KeyMapper customKeysMapper = new CustomeKeyMapper();
        return new TiDBDatabaseSchema(
                dbzTiDBConfig,
                topicSelector,
                isTableIdCaseSensitive,
                customKeysMapper);
    }


    private TableChange readTableSchema(JdbcConnection jdbc, TableId tableId){
        final Map<TableId, TableChanges.TableChange> tableChangeMap = new HashMap<>();
        final String sql = "SHOW CREATE TABLE " + TiDBUtils.quote(tableId);
        try {
            jdbc.query(
                    sql,
                    rs -> {
                        if (rs.next()) {
                            final String ddl = rs.getString(2);
                            final CDCEventOffsetContext offsetContext =
                                    CDCEventOffsetContext.initial(connectorConfig);
                            final TiDBPartition partition =
                                    new TiDBPartition(connectorConfig.getLogicalName());

                            List<SchemaChangeEvent> schemaChangeEvents =
                                    databaseSchema.parseSnapshotDdl(
                                            partition,
                                            ddl,
                                            tableId.catalog(),
                                            offsetContext,
                                            Instant.now());
                            for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
                                for (TableChanges.TableChange tableChange :
                                        schemaChangeEvent.getTableChanges()) {
                                    tableChangeMap.put(tableId, tableChange);
                                }
                            }

                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s by running %s", tableId, sql),
                    e);
        }
        if (!tableChangeMap.containsKey(tableId)) {
            throw new FlinkRuntimeException(
                    String.format("Can't obtain schema for table %s by running %s", tableId, sql));
        }
    }
}
