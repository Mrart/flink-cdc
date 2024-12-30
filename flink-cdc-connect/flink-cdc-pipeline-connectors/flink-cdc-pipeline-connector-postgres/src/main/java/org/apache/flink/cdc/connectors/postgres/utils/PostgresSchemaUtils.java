/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.utils.CustomPostgresSchema;

import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.connection.PostgresConnection;
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

/** Utilities for converting from debezium {@link Table} types to {@link Schema}. */
public class PostgresSchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSchemaUtils.class);

    private static volatile PostgresDialect postgresDialect;

    public static List<String> listSchemas(PostgresSourceConfig sourceConfig, String namespace) {
        try (JdbcConnection jdbc = getPostgresDialect(sourceConfig).openJdbcConnection()) {
            return listSchemas(jdbc, namespace);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list schemas: " + e.getMessage(), e);
        }
    }

    public static List<String> listNamespaces(PostgresSourceConfig sourceConfig) {
        try (JdbcConnection jdbc = getPostgresDialect(sourceConfig).openJdbcConnection()) {
            return listNamespaces(jdbc);
        } catch (SQLException e) {
            throw new RuntimeException("Error to list namespaces: " + e.getMessage(), e);
        }
    }

    public static List<TableId> listTables(
            PostgresSourceConfig sourceConfig, @Nullable String dbName) {
        try (PostgresConnection jdbc = getPostgresDialect(sourceConfig).openJdbcConnection()) {

            List<String> databases =
                    dbName != null
                            ? Collections.singletonList(dbName)
                            : Collections.singletonList(sourceConfig.getDatabaseList().get(0));

            List<TableId> tableIds = new ArrayList<>();
            for (String database : databases) {
                List<TableId> tableIdList =
                        jdbc.getAllTableIds(database).stream()
                                .map(PostgresSchemaUtils::toCdcTableId)
                                .collect(Collectors.toList());
                tableIds.addAll(tableIdList);
            }
            return tableIds;
        } catch (SQLException e) {
            throw new RuntimeException("Error to list databases: " + e.getMessage(), e);
        }
    }

    public static Schema getTableSchema(
            PostgresSourceConfig sourceConfig, PostgresPartition partition, TableId tableId) {
        try (PostgresConnection jdbc = getPostgresDialect(sourceConfig).openJdbcConnection()) {
            return getTableSchema(partition, tableId, sourceConfig, jdbc);
        }
    }

    public static PostgresDialect getPostgresDialect(PostgresSourceConfig sourceConfig) {
        if (postgresDialect == null) { //
            synchronized (PostgresSchemaUtils.class) {
                if (postgresDialect == null) { //
                    postgresDialect = new PostgresDialect(sourceConfig);
                }
            }
        }
        return postgresDialect;
    }

    public static List<String> listSchemas(JdbcConnection jdbc, String namespace)
            throws SQLException {
        LOG.info("Read list of available schemas");
        final List<String> schemaNames = new ArrayList<>();

        String querySql =
                String.format(
                        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = %s",
                        quote(namespace));

        jdbc.query(
                querySql,
                rs -> {
                    while (rs.next()) {
                        schemaNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available schemas are: {}", schemaNames);
        return schemaNames;
    }

    public static List<String> listNamespaces(JdbcConnection jdbc) throws SQLException {
        LOG.info("Read list of available namespaces");
        final List<String> namespaceNames = new ArrayList<>();
        jdbc.query(
                "SELECT DATNAME FROM PG_DATABASE",
                rs -> {
                    while (rs.next()) {
                        namespaceNames.add(rs.getString(1));
                    }
                });
        LOG.info("\t list of available namespaces are: {}", namespaceNames);
        return namespaceNames;
    }

    public static String quote(String dbOrTableName) {
        return "\"" + dbOrTableName + "\"";
    }

    public static Schema getTableSchema(
            PostgresPartition partition,
            TableId tableId,
            PostgresSourceConfig sourceConfig,
            PostgresConnection jdbc) {
        // fetch table schemas
        CustomPostgresSchema postgresSchema = new CustomPostgresSchema(jdbc, sourceConfig);
        TableChanges.TableChange tableSchema = postgresSchema.getTableSchema(toDbzTableId(tableId));
        return toSchema(tableSchema.getTable());
    }

    public static Schema getTableSchema(
            io.debezium.relational.TableId tableId,
            PostgresSourceConfig sourceConfig,
            PostgresConnection jdbc) {
        // fetch table schemas
        CustomPostgresSchema postgresSchema = new CustomPostgresSchema(jdbc, sourceConfig);

        TableChanges.TableChange tableSchema = postgresSchema.getTableSchema(tableId);
        return toSchema(tableSchema.getTable());
    }

    public static Schema toSchema(Table table) {
        List<Column> columns =
                table.columns().stream()
                        .map(PostgresSchemaUtils::toColumn)
                        .collect(Collectors.toList());

        return Schema.newBuilder()
                .setColumns(columns)
                .primaryKey(table.primaryKeyColumnNames())
                .comment(table.comment())
                .build();
    }

    public static Column toColumn(io.debezium.relational.Column column) {
        return Column.physicalColumn(
                column.name(), PostgresTypeUtils.fromDbzColumn(column), column.comment());
    }

    public static io.debezium.relational.TableId toDbzTableId(TableId tableId) {
        return new io.debezium.relational.TableId(
                tableId.getSchemaName(), null, tableId.getTableName());
    }

    public static TableId toCdcTableId(io.debezium.relational.TableId dbzTableId) {
        return TableId.tableId(dbzTableId.schema(), dbzTableId.table());
    }

    private PostgresSchemaUtils() {}
}