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

    public TableChange getTableSchema(
            TiDBPartition partition, JdbcConnection jdbc, TableId tableId) {
        // read schema from cache first
        TableChanges.TableChange schema = schemasByTableId.get(tableId);
        if (schema == null) {
            schema = buildTableSchema(partition, jdbc, tableId);
            schemasByTableId.put(tableId, schema);
        }
        return schema;
    }

    private TableChange buildTableSchema(
            TiDBPartition partition, JdbcConnection jdbc, TableId tableId) {
        final Map<TableId, TableChange> tableChangeMap = new HashMap<>();
        String showCreateTable = SHOW_CREATE_TABLE + TiDBUtils.quote(tableId);
        buildSchemaByShowCreateTable(partition, jdbc, tableId, tableChangeMap);
        if (!tableChangeMap.containsKey(tableId)) {
            // fallback to desc table
            String descTable = DESC_TABLE +TiDBUtils.quote(tableId);
            buildSchemaByDescTable(partition, jdbc, descTable, tableId, tableChangeMap);
            if (!tableChangeMap.containsKey(tableId)) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Can't obtain schema for table %s by running %s and %s ",
                                tableId, showCreateTable, descTable));
            }
        }
        return tableChangeMap.get(tableId);
    }


    private void buildSchemaByShowCreateTable(
            TiDBPartition partition,
            JdbcConnection jdbc,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        final String sql = SHOW_CREATE_TABLE + TiDBUtils.quote(tableId);
        try {
            jdbc.query(
                    sql,
                    rs -> {
                        if (rs.next()) {
                            final String ddl = rs.getString(2);
                            parseSchemaByDdl(partition, ddl, tableId, tableChangeMap);
                        }
                    });
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format("Failed to read schema for table %s by running %s", tableId, sql),
                    e);
        }
    }

    private void parseSchemaByDdl(
            TiDBPartition partition,
            String ddl,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        //todo databaseSchema.parseSnapshotDdl  和 CDCEventOffsetContext.initial
        final CDCEventOffsetContext offsetContext = CDCEventOffsetContext.initial(connectorConfig);
        List<SchemaChangeEvent> schemaChangeEvents =
                databaseSchema.parseSnapshotDdl(
                        partition, ddl, tableId.catalog(), offsetContext, Instant.now());
        for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
            for (TableChange tableChange : schemaChangeEvent.getTableChanges()) {
                tableChangeMap.put(tableId, tableChange);
            }
        }
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

    private void buildSchemaByDescTable(
            TiDBPartition partition,
            JdbcConnection jdbc,
            String descTable,
            TableId tableId,
            Map<TableId, TableChange> tableChangeMap) {
        List<TiDBFieldDefinition> fieldMetas = new ArrayList<>();
        List<String> primaryKeys = new ArrayList<>();
        try {
            jdbc.query(
                    descTable,
                    rs -> {
                        while (rs.next()) {
                            TiDBFieldDefinition meta = new TiDBFieldDefinition();
                            meta.setColumnName(rs.getString("Field"));
                            meta.setColumnType(rs.getString("Type"));
                            meta.setNullable(
                                    StringUtils.equalsIgnoreCase(rs.getString("Null"), "YES"));
                            meta.setKey("PRI".equalsIgnoreCase(rs.getString("Key")));
                            meta.setUnique("UNI".equalsIgnoreCase(rs.getString("Key")));
                            meta.setDefaultValue(rs.getString("Default"));
                            meta.setExtra(rs.getString("Extra"));
                            if (meta.isKey()) {
                                primaryKeys.add(meta.getColumnName());
                            }
                            fieldMetas.add(meta);
                        }
                    });
            parseSchemaByDdl(
                    partition,
                    new TiDBTableDefinition(tableId, fieldMetas, primaryKeys).toDdl(),
                    tableId,
                    tableChangeMap);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Failed to read schema for table %s by running %s", tableId, descTable),
                    e);
        }
    }


    //base中方法
//    private TableChange readTableSchema(JdbcConnection jdbc, TableId tableId) throws SQLException {
//        final Map<TableId, TableChanges.TableChange> tableChangeMap = new HashMap<>();
//        final String sql = "SHOW CREATE TABLE " + TiDBUtils.quote(tableId);
//        try {
//            jdbc.query(
//                    sql,
//                    rs -> {
//                        if (rs.next()) {
//                            final String ddl = rs.getString(2);
//                            final CDCEventOffsetContext offsetContext =
//                                    CDCEventOffsetContext.initial(dbzConfig);
//                            final TiDBPartition partition =
//                                    new TiDBPartition(dbzConfig.getLogicalName());
//
//                            //用 CustomPostgresSchema中的 进行修改？
//                            List<SchemaChangeEvent> schemaChangeEvents =
//                                    databaseSchema.parseSnapshotDdl(
//                                            partition,
//                                            ddl,
//                                            tableId.catalog(),
//                                            offsetContext,
//                                            Instant.now());
//                            for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
//                                for (TableChanges.TableChange tableChange :
//                                        schemaChangeEvent.getTableChanges()) {
//                                    tableChangeMap.put(tableId, tableChange);
//                                }                            }
//
//                        }
//                    });
//        } catch (SQLException e) {
//            throw new FlinkRuntimeException(
//                    String.format("Failed to read schema for table %s by running %s", tableId, sql),
//                    e);
//        }
//        if (!tableChangeMap.containsKey(tableId)) {
//            throw new FlinkRuntimeException(
//                    String.format("Can't obtain schema for table %s by running %s", tableId, sql));
//        }
//    }
}
