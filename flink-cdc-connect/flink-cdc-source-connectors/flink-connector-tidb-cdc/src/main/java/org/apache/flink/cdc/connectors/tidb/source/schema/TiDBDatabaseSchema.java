package org.apache.flink.cdc.connectors.tidb.source.schema;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBDefaultValueConverter;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBValueConverters;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;

import io.debezium.connector.tidb.TiDBAntlrDdlParser;
import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.relational.*;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.text.MultipleParsingExceptions;
import io.debezium.text.ParsingException;
import io.debezium.util.Collect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.*;

/** OceanBase database schema. */
public class TiDBDatabaseSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDBDatabaseSchema.class);
    private final Set<String> ignoredQueryStatements =
            Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
    //    private final DdlParser ddlParser;
    private final RelationalTableFilters filters;
    private final DdlParser ddlParser;
    private final DdlChanges ddlChanges;

    public TiDBDatabaseSchema(
            TiDBConnectorConfig config,
            TiDBValueConverters tiDBValueConverters,
            TopicSelector<TableId> topicSelector,
            boolean tableIdCaseInsensitive) {
        super(
                config,
                topicSelector,
                config.getTableFilters().dataCollectionFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        tiDBValueConverters,
                        new TiDBDefaultValueConverter(tiDBValueConverters),
                        config.schemaNameAdjustmentMode().createAdjuster(),
                        config.customConverterRegistry(),
                        config.getSourceInfoStructMaker().schema(),
                        config.getSanitizeFieldNames(),
                        false),
                tableIdCaseInsensitive,
                config.getKeyMapper());

        // todo  change
        this.ddlParser =
                new TiDBAntlrDdlParser(
                        true,
                        false,
                        config.isSchemaCommentsHistoryEnabled(),
                        tiDBValueConverters,
                        getTableFilter());
        filters = config.getTableFilters();
        this.ddlChanges = this.ddlParser.getDdlChanges();
    }

    public TiDBDatabaseSchema refresh(
            TiDBConnection connection, TiDBConnectorConfig config, boolean printReplicaIdentityInfo)
            throws SQLException {
        // read all the information from the DB
        //        connection.readSchema(tables(), null, null, getTableFilter(), null, true);
        //        LOGGER.info("TiDBDatabaseSchema refresh **********");
        connection.readTiDBSchema(config, this, tables(), null, null, getTableFilter(), null, true);

        //    if (printReplicaIdentityInfo) {
        //      // print out all the replica identity info
        //      tableIds().forEach(tableId -> printReplicaIdentityInfo(connection, tableId));
        //    }
        // and then refresh the schemas
        refreshSchemas();
        //    if (readToastableColumns) {
        //      tableIds().forEach(tableId -> refreshToastableColumnsMap(connection, tableId));
        //    }
        return this;
    }

    protected void refreshSchemas() {
        clearSchemas();
        // Create TableSchema instances for any existing table ...
        tableIds().forEach(this::refreshSchema);
    }

    @Override
    protected void refreshSchema(TableId id) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("refreshing DB schema for table '{}'", id);
        }
        Table table = tableFor(id);
        buildAndRegisterSchema(table);
    }

    public List<SchemaChangeEvent> parseSnapshotDdl(
            TiDBPartition partition,
            String ddlStatements,
            String databaseName,
            CDCEventOffsetContext offset,
            Instant sourceTime) {
        LOGGER.debug("Processing snapshot DDL '{}' for database '{}'", ddlStatements, databaseName);
        return parseDdl(partition, ddlStatements, databaseName, offset, sourceTime, true);
    }

    private List<SchemaChangeEvent> parseDdl(
            TiDBPartition partition,
            String ddlStatements,
            String databaseName,
            CDCEventOffsetContext offset,
            Instant sourceTime,
            boolean snapshot) {
        final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(3);

        if (ignoredQueryStatements.contains(ddlStatements)) {
            return schemaChangeEvents;
        }

        try {
            this.ddlChanges.reset();
            this.ddlParser.setCurrentSchema(databaseName);
            this.ddlParser.parse(ddlStatements, tables());
        } catch (ParsingException | MultipleParsingExceptions e) {
            throw e;
        }
        if (!ddlChanges.isEmpty()) {
            ddlChanges.getEventsByDatabase(
                    (String dbName, List<DdlParserListener.Event> events) -> {
                        final String sanitizedDbName = (dbName == null) ? "" : dbName;
                        if (acceptableDatabase(dbName)) {
                            final Set<TableId> tableIds = new HashSet<>();
                            events.forEach(
                                    event -> {
                                        final TableId tableId = getTableId(event);
                                        if (tableId != null) {
                                            tableIds.add(tableId);
                                        }
                                    });
                            events.forEach(
                                    event -> {
                                        final TableId tableId = getTableId(event);
                                        offset.tableEvent(dbName, tableIds, sourceTime);
                                        // For SET with multiple parameters
                                        if (event instanceof DdlParserListener.TableCreatedEvent) {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType.CREATE,
                                                    snapshot);
                                        } else if (event
                                                        instanceof
                                                        DdlParserListener.TableAlteredEvent
                                                || event
                                                        instanceof
                                                        DdlParserListener.TableIndexCreatedEvent
                                                || event
                                                        instanceof
                                                        DdlParserListener.TableIndexDroppedEvent) {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType.ALTER,
                                                    snapshot);
                                        } else if (event
                                                instanceof DdlParserListener.TableDroppedEvent) {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType.DROP,
                                                    snapshot);
                                        } else if (event
                                                instanceof DdlParserListener.SetVariableEvent) {
                                            // SET statement with multiple variable emits event for
                                            // each variable. We want to emit only
                                            // one change event
                                            final DdlParserListener.SetVariableEvent varEvent =
                                                    (DdlParserListener.SetVariableEvent) event;
                                            if (varEvent.order() == 0) {
                                                emitChangeEvent(
                                                        partition,
                                                        offset,
                                                        schemaChangeEvents,
                                                        sanitizedDbName,
                                                        event,
                                                        tableId,
                                                        SchemaChangeEvent.SchemaChangeEventType
                                                                .DATABASE,
                                                        snapshot);
                                            }
                                        } else {
                                            emitChangeEvent(
                                                    partition,
                                                    offset,
                                                    schemaChangeEvents,
                                                    sanitizedDbName,
                                                    event,
                                                    tableId,
                                                    SchemaChangeEvent.SchemaChangeEventType
                                                            .DATABASE,
                                                    snapshot);
                                        }
                                    });
                        }
                    });
        } else {
            offset.databaseEvent(databaseName, sourceTime);
            schemaChangeEvents.add(
                    SchemaChangeEvent.ofDatabase(
                            partition, offset, databaseName, ddlStatements, snapshot));
        }
        return schemaChangeEvents;
    }

    private boolean acceptableDatabase(final String databaseName) {
        return filters.databaseFilter().test(databaseName)
                || databaseName == null
                || databaseName.isEmpty();
    }

    private TableId getTableId(DdlParserListener.Event event) {
        if (event instanceof DdlParserListener.TableEvent) {
            return ((DdlParserListener.TableEvent) event).tableId();
        } else if (event instanceof DdlParserListener.TableIndexEvent) {
            return ((DdlParserListener.TableIndexEvent) event).tableId();
        }
        return null;
    }

    private void emitChangeEvent(
            TiDBPartition partition,
            CDCEventOffsetContext offset,
            List<SchemaChangeEvent> schemaChangeEvents,
            final String sanitizedDbName,
            DdlParserListener.Event event,
            TableId tableId,
            SchemaChangeEvent.SchemaChangeEventType type,
            boolean snapshot) {
        SchemaChangeEvent schemaChangeEvent;
        if (type.equals(SchemaChangeEvent.SchemaChangeEventType.ALTER)
                && event instanceof DdlParserListener.TableAlteredEvent
                && ((DdlParserListener.TableAlteredEvent) event).previousTableId() != null) {
            schemaChangeEvent =
                    SchemaChangeEvent.ofRename(
                            partition,
                            offset,
                            sanitizedDbName,
                            null,
                            event.statement(),
                            tableId != null ? tableFor(tableId) : null,
                            ((DdlParserListener.TableAlteredEvent) event).previousTableId());
        } else {
            schemaChangeEvent =
                    SchemaChangeEvent.of(
                            type,
                            partition,
                            offset,
                            sanitizedDbName,
                            null,
                            event.statement(),
                            tableId != null ? tableFor(tableId) : null,
                            snapshot);
        }
        schemaChangeEvents.add(schemaChangeEvent);
    }

    //  private void printReplicaIdentityInfo(TiDBConnection connection, TableId tableId) {
    //    try {
    //      ServerInfo.ReplicaIdentity replicaIdentity =
    // connection.readReplicaIdentityInfo(tableId);
    //      LOGGER.info("REPLICA IDENTITY for '{}' is '{}'; {}", tableId, replicaIdentity,
    // replicaIdentity.description());
    //    }
    //    catch (SQLException e) {
    //      LOGGER.warn("Cannot determine REPLICA IDENTITY info for '{}'", tableId);
    //    }
    //  }
    //  protected void refresh(TiDBConnection connection, TableId tableId, boolean
    // refreshToastableColumns) throws SQLException {
    //    Tables temp = new Tables();
    //    connection.readSchema(temp, null, null, tableId::equals, null, true);
    //
    //    // the table could be deleted before the event was processed
    //    if (temp.size() == 0) {
    //      LOGGER.warn("Refresh of {} was requested but the table no longer exists", tableId);
    //      return;
    //    }
    //    // overwrite (add or update) or views of the tables
    //    tables().overwriteTable(temp.forTable(tableId));
    //    // refresh the schema
    //    refreshSchema(tableId);
    //
    //    if (refreshToastableColumns) {
    //      // and refresh toastable columns info
    //      refreshToastableColumnsMap(connection, tableId);
    //    }
    //  }

    //    private void refreshToastableColumnsMap(TiDBConnection connection, TableId tableId) {
    //        // This method populates the list of 'toastable' columns for `tableId`.
    //        // A toastable column is one that has storage strategy 'x' (inline-compressible +
    // secondary
    //        // storage enabled),
    //        // 'e' (secondary storage enabled), or 'm' (inline-compressible).
    //        //
    //        // Note that, rather confusingly, the 'm' storage strategy does in fact permit
    // secondary
    //        // storage, but only as a
    //        // last resort.
    //        //
    //        // Also, we cannot account for the possibility that future versions of PostgreSQL
    // introduce
    //        // new storage strategies
    //        // that include secondary storage. We should move to native decoding in PG 10 and get
    // rid of
    //        // this hacky code
    //        // before that possibility is realized.
    //
    //        // Collect the non-system (attnum > 0), present (not attisdropped) column names that
    // are
    //        // toastable.
    //        //
    //        // NOTE (Ian Axelrod):
    //        // I Would prefer to use data provided by PgDatabaseMetaData, but the PG JDBC driver
    // does
    //        // not expose storage type
    //        // information. Thus, we need to make a separate query. If we are refreshing schemas
    // rarely,
    //        // this is not a big
    //        // deal.
    //        List<String> toastableColumns = new ArrayList<>();
    //        String relName = tableId.table();
    //        String schema =
    //                tableId.schema() != null && tableId.schema().length() > 0
    //                        ? tableId.schema()
    //                        : "public";
    //        String statement =
    //                "select att.attname"
    //                        + " from pg_attribute att "
    //                        + " join pg_class tbl on tbl.oid = att.attrelid"
    //                        + " join pg_namespace ns on tbl.relnamespace = ns.oid"
    //                        + " where tbl.relname = ?"
    //                        + " and ns.nspname = ?"
    //                        + " and att.attnum > 0"
    //                        + " and att.attstorage in ('x', 'e', 'm')"
    //                        + " and not att.attisdropped;";
    //
    //        try {
    //            connection.prepareQuery(
    //                    statement,
    //                    stmt -> {
    //                        stmt.setString(1, relName);
    //                        stmt.setString(2, schema);
    //                    },
    //                    rs -> {
    //                        while (rs.next()) {
    //                            toastableColumns.add(rs.getString(1));
    //                        }
    //                    });
    //            if (!connection.connection().getAutoCommit()) {
    //                connection.connection().commit();
    //            }
    //        } catch (SQLException e) {
    //            throw new ConnectException("Unable to refresh toastable columns mapping", e);
    //        }
    //        //    tableIdToToastableColumns.put(tableId,
    //        // Collections.unmodifiableList(toastableColumns));
    //    }
    //
    //    public boolean isGlobalSetVariableStatement(String ddl, String databaseName) {
    //        return (databaseName == null || databaseName.isEmpty())
    //                && ddl != null
    //                && ddl.toUpperCase().startsWith("SET ");
    //    }
    //
    //    private boolean acceptableDatabase(final String databaseName) {
    //        return filters.databaseFilter().test(databaseName)
    //                || databaseName == null
    //                || databaseName.isEmpty();
    //    }
    //
    //    private TableId getTableId(DdlParserListener.Event event) {
    //        if (event instanceof DdlParserListener.TableEvent) {
    //            return ((DdlParserListener.TableEvent) event).tableId();
    //        } else if (event instanceof DdlParserListener.TableIndexEvent) {
    //            return ((DdlParserListener.TableIndexEvent) event).tableId();
    //        }
    //        return null;
    //    }

    //    private void emitChangeEvent(
    //            TiDBPartition partition,
    //            MySqlOffsetContext offset,
    //            List<SchemaChangeEvent> schemaChangeEvents,
    //            final String sanitizedDbName,
    //            DdlParserListener.Event event,
    //            TableId tableId,
    //            SchemaChangeEvent.SchemaChangeEventType type,
    //            boolean snapshot) {
    //        SchemaChangeEvent schemaChangeEvent;
    //        if (type.equals(SchemaChangeEvent.SchemaChangeEventType.ALTER)
    //                && event instanceof DdlParserListener.TableAlteredEvent
    //                && ((DdlParserListener.TableAlteredEvent) event).previousTableId() != null) {
    //            schemaChangeEvent =
    //                    SchemaChangeEvent.ofRename(
    //                            partition,
    //                            offset,
    //                            sanitizedDbName,
    //                            null,
    //                            event.statement(),
    //                            tableId != null ? tableFor(tableId) : null,
    //                            ((DdlParserListener.TableAlteredEvent) event).previousTableId());
    //        } else {
    //            schemaChangeEvent =
    //                    SchemaChangeEvent.of(
    //                            type,
    //                            partition,
    //                            offset,
    //                            sanitizedDbName,
    //                            null,
    //                            event.statement(),
    //                            tableId != null ? tableFor(tableId) : null,
    //                            snapshot);
    //        }
    //        schemaChangeEvents.add(schemaChangeEvent);
    //    }

    //    private List<SchemaChangeEvent> parseDdl(
    //            TiDBPartition partition,
    //            String ddlStatements,
    //            String databaseName,
    //            MySqlOffsetContext offset,
    //            Instant sourceTime,
    //            boolean snapshot) {
    //        final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(3);
    //
    //        if (ignoredQueryStatements.contains(ddlStatements)) {
    //            return schemaChangeEvents;
    //        }
    //
    //        this.ddlChanges.reset();
    //        this.ddlParser.setCurrentSchema(databaseName);
    //        this.ddlParser.parse(ddlStatements, tables());
    //
    //        //    catch (ParsingException | MultipleParsingExceptions e) {
    //        //      if (databaseHistory.skipUnparseableDdlStatements()) {
    //        //        LOGGER.warn("Ignoring unparseable DDL statement '{}': {}", ddlStatements,
    // e);
    //        //      }
    //        //      else {
    //        //        throw e;
    //        //      }
    //        //    }
    //
    //        // No need to send schema events or store DDL if no table has changed
    //        if (isGlobalSetVariableStatement(ddlStatements, databaseName)
    //                || ddlChanges.anyMatch(filters)) {
    //
    //            // We are supposed to _also_ record the schema changes as SourceRecords, but these
    // need
    //            // to be filtered
    //            // by database. Unfortunately, the databaseName on the event might not be the same
    //            // database as that
    //            // being modified by the DDL statements (since the DDL statements can have
    //            // fully-qualified names).
    //            // Therefore, we have to look at each statement to figure out which database it
    // applies
    //            // and then
    //            // record the DDL statements (still in the same order) to those databases.
    //            if (!ddlChanges.isEmpty()) {
    //                // We understood at least some of the DDL statements and can figure out to
    // which
    //                // database they apply.
    //                // They also apply to more databases than 'databaseName', so we need to apply
    // the
    //                // DDL statements in
    //                // the same order they were read for each _affected_ database, grouped
    // together if
    //                // multiple apply
    //                // to the same _affected_ database...
    //                ddlChanges.getEventsByDatabase(
    //                        (String dbName, List<DdlParserListener.Event> events) -> {
    //                            final String sanitizedDbName = (dbName == null) ? "" : dbName;
    //                            // acceptableDatabase(dbName)
    //                            if (acceptableDatabase(dbName)) {
    //                                final Set<TableId> tableIds = new HashSet<>();
    //                                events.forEach(
    //                                        event -> {
    //                                            final TableId tableId = getTableId(event);
    //                                            if (tableId != null) {
    //                                                tableIds.add(tableId);
    //                                            }
    //                                        });
    //                                events.forEach(
    //                                        event -> {
    //                                            final TableId tableId = getTableId(event);
    //                                            offset.tableEvent(dbName, tableIds, sourceTime);
    //                                            // For SET with multiple parameters
    //                                            if (event
    //                                                    instanceof
    //                                                    DdlParserListener.TableCreatedEvent) {
    //                                                emitChangeEvent(
    //                                                        partition,
    //                                                        offset,
    //                                                        schemaChangeEvents,
    //                                                        sanitizedDbName,
    //                                                        event,
    //                                                        tableId,
    //
    // SchemaChangeEvent.SchemaChangeEventType
    //                                                                .CREATE,
    //                                                        snapshot);
    //                                            } else if (event
    //                                                            instanceof
    //
    // DdlParserListener.TableAlteredEvent
    //                                                    || event
    //                                                            instanceof
    //
    // DdlParserListener.TableIndexCreatedEvent
    //                                                    || event
    //                                                            instanceof
    //                                                            DdlParserListener
    //                                                                    .TableIndexDroppedEvent) {
    //                                                emitChangeEvent(
    //                                                        partition,
    //                                                        offset,
    //                                                        schemaChangeEvents,
    //                                                        sanitizedDbName,
    //                                                        event,
    //                                                        tableId,
    //
    // SchemaChangeEvent.SchemaChangeEventType
    //                                                                .ALTER,
    //                                                        snapshot);
    //                                            } else if (event
    //                                                    instanceof
    //                                                    DdlParserListener.TableDroppedEvent) {
    //                                                emitChangeEvent(
    //                                                        partition,
    //                                                        offset,
    //                                                        schemaChangeEvents,
    //                                                        sanitizedDbName,
    //                                                        event,
    //                                                        tableId,
    //
    // SchemaChangeEvent.SchemaChangeEventType
    //                                                                .DROP,
    //                                                        snapshot);
    //                                            } else if (event
    //                                                    instanceof
    // DdlParserListener.SetVariableEvent) {
    //                                                // SET statement with multiple variable emits
    // event
    //                                                // for each variable. We want to emit only
    //                                                // one change event
    //                                                final DdlParserListener.SetVariableEvent
    // varEvent =
    //                                                        (DdlParserListener.SetVariableEvent)
    // event;
    //                                                if (varEvent.order() == 0) {
    //                                                    emitChangeEvent(
    //                                                            partition,
    //                                                            offset,
    //                                                            schemaChangeEvents,
    //                                                            sanitizedDbName,
    //                                                            event,
    //                                                            tableId,
    //
    // SchemaChangeEvent.SchemaChangeEventType
    //                                                                    .DATABASE,
    //                                                            snapshot);
    //                                                }
    //                                            } else {
    //                                                emitChangeEvent(
    //                                                        partition,
    //                                                        offset,
    //                                                        schemaChangeEvents,
    //                                                        sanitizedDbName,
    //                                                        event,
    //                                                        tableId,
    //
    // SchemaChangeEvent.SchemaChangeEventType
    //                                                                .DATABASE,
    //                                                        snapshot);
    //                                            }
    //                                        });
    //                            }
    //                        });
    //            } else {
    //                offset.databaseEvent(databaseName, sourceTime);
    //                schemaChangeEvents.add(
    //                        SchemaChangeEvent.ofDatabase(
    //                                partition, offset, databaseName, ddlStatements, snapshot));
    //            }
    //        } else {
    //            LOGGER.debug(
    //                    "Changes for DDL '{}' were filtered and not recorded in database history",
    //                    ddlStatements);
    //        }
    //        return schemaChangeEvents;
    //    }
}
