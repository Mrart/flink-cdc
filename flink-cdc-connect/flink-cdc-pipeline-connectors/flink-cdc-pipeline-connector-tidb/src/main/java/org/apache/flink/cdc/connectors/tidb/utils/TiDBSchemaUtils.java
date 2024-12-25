package org.apache.flink.cdc.connectors.tidb.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBSchema;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Table;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TiDBSchemaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBSchemaUtils.class);

    private static volatile TiDBDialect tiDBDialect;

    public static List<String> listDatabase(TiDBSourceConfig tiDBSourceConfig) {
        try (JdbcConnection jdbc = getTiDBDialect(tiDBSourceConfig).openJdbcConnection()) {
            return listDatabases(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            TiDBSourceConfig tiDBSourceConfig, @Nullable String dbName) {
        try (JdbcConnection jdbc = getTiDBDialect(tiDBSourceConfig).openJdbcConnection()) {
            List<String> databases =
                    dbName != null ? Collections.singletonList(dbName) : listDatabases(jdbc);

            List<TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                tableIds.addAll(listTables(jdbc, database));
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static TiDBDialect getTiDBDialect(TiDBSourceConfig sourceConfig) {
        if (tiDBDialect == null) { //
            synchronized (TiDBSchemaUtils.class) {
                if (tiDBDialect == null) { //
                    tiDBDialect = new TiDBDialect(sourceConfig);
                }
            }
        }
        return tiDBDialect;
    }

    public static List<String> listDatabases(JdbcConnection jdbc) throws SQLException {
        // -------------------
        // READ DATABASE NAMES
        // -------------------
        // Get the list of databases ...
        LOG.info("Read list of available databases");
        final List<String> databaseNames = new ArrayList<>();
        jdbc.query(
                "SHOW DATABASES WHERE `database` NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')",
                rs -> {
                    while (rs.next()) {
                        databaseNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available databases are: {}", databaseNames);
        return databaseNames;
    }

    public static List<TableId> listTables(JdbcConnection jdbc, String dbName) throws SQLException {
        // ----------------
        // READ TABLE NAMES
        // ----------------
        // Get the list of table IDs for each database. We can't use a prepared statement with
        // MySQL, so we have to build the SQL statement each time. Although in other cases this
        // might lead to SQL injection, in our case we are reading the database names from the
        // database and not taking them from the user ...
        LOG.info("Read list of available tables in {}", dbName);
        final List<TableId> tableIds = new ArrayList<>();
        jdbc.query(
                "SHOW FULL TABLES IN " + quote(dbName) + " where Table_Type = 'BASE TABLE'",
                rs -> {
                    while (rs.next()) {
                        tableIds.add(TableId.tableId(dbName, rs.getString(1)));
                    }
                });
        LOG.info("\t list of available tables are: {}", tableIds);
        return tableIds;
    }

    public static String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    public static Schema getTableSchema(
            TiDBSourceConfig tiDBSourceConfig, io.debezium.relational.TableId tableId)
            throws SQLException {
        // fetch table schemas
        try (TiDBConnection jdbc = getTiDBDialect(tiDBSourceConfig).openJdbcConnection()) {
            return getTableSchema(tableId, tiDBSourceConfig, jdbc);
        }
    }

    public static Schema getTableSchema(
            io.debezium.relational.TableId tableId,
            TiDBSourceConfig tiDBSourceConfig,
            TiDBConnection jdbc) {
        // fetch table schemas
        TiDBSchema tiDBSchema =
                new TiDBSchema(tiDBSourceConfig, TiDBConnectionUtils.isTableIdCaseSensitive(jdbc));
        TableChanges.TableChange tableSchema = tiDBSchema.getTableSchema(jdbc, tableId);
        return toSchema(tableSchema.getTable());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(TiDBSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(), TiDBTypeUtils.fromDbzColumn(column), column.comment());
    }
}
