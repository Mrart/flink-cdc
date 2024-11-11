package org.apache.flink.cdc.connectors.tidb.source.schema;

import com.fasterxml.jackson.databind.util.BeanUtil;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
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
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBValueConverters;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** OceanBase database schema. */
public class TiDBDatabaseSchema extends RelationalDatabaseSchema {

  private final static Logger LOGGER = LoggerFactory.getLogger(MySqlDatabaseSchema.class);
  private final Set<String> ignoredQueryStatements = Collect.unmodifiableSet("BEGIN", "END", "FLUSH PRIVILEGES");
  private final DdlParser ddlParser;
  private final RelationalTableFilters filters;
  private final DdlChanges ddlChanges;

  public TiDBDatabaseSchema(
          TiDBConnectorConfig config,
          TopicSelector<TableId> topicSelector,
          boolean tableIdCaseInsensitive,
          Key.KeyMapper customKeysMapper) {
    super(
            config,
            topicSelector,
            config.getTableFilters().dataCollectionFilter(),
            config.getColumnFilter(),
            new TableSchemaBuilder(
                    new TiDBValueConverters(config),
                    config.schemaNameAdjustmentMode().createAdjuster(),
                    config.customConverterRegistry(),
                    config.getSourceInfoStructMaker().schema(),
                    config.getSanitizeFieldNames(),
                    false),
            tableIdCaseInsensitive,
            customKeysMapper);


    //todo  change
    this.ddlParser = new TiDBAntlrDdlParser(
            true,
            false,
            config.isSchemaCommentsHistoryEnabled(),
            new TiDBValueConverters(config),
            getTableFilter());

    filters = config.getTableFilters();

    ddlChanges = ddlParser.getDdlChanges();
  }

  public TiDBDatabaseSchema(
          TiDBConnectorConfig config,
          TopicSelector<TableId> topicSelector,
          boolean tableIdCaseInsensitive) {
    super(
            config,
            topicSelector,
            config.getTableFilters().dataCollectionFilter(),
            config.getColumnFilter(),
            new TableSchemaBuilder(
                    new TiDBValueConverters(config),
                    config.schemaNameAdjustmentMode().createAdjuster(),
                    config.customConverterRegistry(),
                    config.getSourceInfoStructMaker().schema(),
                    config.getSanitizeFieldNames(),
                    false),
            tableIdCaseInsensitive,
            config.getKeyMapper());

    //todo  change
    this.ddlParser = new TiDBAntlrDdlParser(
            true,
            false,
            config.isSchemaCommentsHistoryEnabled(),
            new TiDBValueConverters(config),
            getTableFilter());

    filters = config.getTableFilters();

    ddlChanges = ddlParser.getDdlChanges();


  }

  public boolean isGlobalSetVariableStatement(String ddl, String databaseName) {
    return (databaseName == null || databaseName.isEmpty()) && ddl != null && ddl.toUpperCase().startsWith("SET ");
  }

  private boolean acceptableDatabase(final String databaseName) {
    return filters.databaseFilter().test(databaseName)
            || databaseName == null
            || databaseName.isEmpty();
  }

  private TableId getTableId(DdlParserListener.Event event) {
    if (event instanceof DdlParserListener.TableEvent) {
      return ((DdlParserListener.TableEvent) event).tableId();
    }
    else if (event instanceof DdlParserListener.TableIndexEvent) {
      return ((DdlParserListener.TableIndexEvent) event).tableId();
    }
    return null;
  }

  private void emitChangeEvent(TiDBPartition partition, CDCEventOffsetContext offset, List<SchemaChangeEvent> schemaChangeEvents,
                               final String sanitizedDbName, DdlParserListener.Event event, TableId tableId, SchemaChangeEvent.SchemaChangeEventType type,
                               boolean snapshot) {
    SchemaChangeEvent schemaChangeEvent;
    if (type.equals(SchemaChangeEvent.SchemaChangeEventType.ALTER) && event instanceof DdlParserListener.TableAlteredEvent
            && ((DdlParserListener.TableAlteredEvent) event).previousTableId() != null) {
      schemaChangeEvent = SchemaChangeEvent.ofRename(
              partition,
              offset,
              sanitizedDbName,
              null,
              event.statement(),
              tableId != null ? tableFor(tableId) : null,
              ((DdlParserListener.TableAlteredEvent) event).previousTableId());
    }
    else {
      schemaChangeEvent = SchemaChangeEvent.of(
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

  public List<SchemaChangeEvent> parseSnapshotDdl(TiDBPartition partition, String ddlStatements, String databaseName,
                                                  CDCEventOffsetContext offset, Instant sourceTime) {
    LOGGER.debug("Processing snapshot DDL '{}' for database '{}'", ddlStatements, databaseName);
    return parseDdl(partition, ddlStatements, databaseName, offset, sourceTime, true);
  }

  private List<SchemaChangeEvent> parseDdl(TiDBPartition partition, String ddlStatements, String databaseName,
                                           CDCEventOffsetContext offset, Instant sourceTime, boolean snapshot) {
    final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(3);

    if (ignoredQueryStatements.contains(ddlStatements)) {
      return schemaChangeEvents;
    }

      this.ddlChanges.reset();
      this.ddlParser.setCurrentSchema(databaseName);
      this.ddlParser.parse(ddlStatements, tables());

//    catch (ParsingException | MultipleParsingExceptions e) {
//      if (databaseHistory.skipUnparseableDdlStatements()) {
//        LOGGER.warn("Ignoring unparseable DDL statement '{}': {}", ddlStatements, e);
//      }
//      else {
//        throw e;
//      }
//    }

    // No need to send schema events or store DDL if no table has changed
    if (isGlobalSetVariableStatement(ddlStatements, databaseName) || ddlChanges.anyMatch(filters)) {

      // We are supposed to _also_ record the schema changes as SourceRecords, but these need to be filtered
      // by database. Unfortunately, the databaseName on the event might not be the same database as that
      // being modified by the DDL statements (since the DDL statements can have fully-qualified names).
      // Therefore, we have to look at each statement to figure out which database it applies and then
      // record the DDL statements (still in the same order) to those databases.
      if (!ddlChanges.isEmpty()) {
        // We understood at least some of the DDL statements and can figure out to which database they apply.
        // They also apply to more databases than 'databaseName', so we need to apply the DDL statements in
        // the same order they were read for each _affected_ database, grouped together if multiple apply
        // to the same _affected_ database...
        ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
          final String sanitizedDbName = (dbName == null) ? "" : dbName;
          //acceptableDatabase(dbName)
          if (acceptableDatabase(dbName)) {
            final Set<TableId> tableIds = new HashSet<>();
            events.forEach(event -> {
              final TableId tableId = getTableId(event);
              if (tableId != null) {
                tableIds.add(tableId);
              }
            });
            events.forEach(event -> {
              final TableId tableId = getTableId(event);
              offset.tableEvent(dbName, tableIds, sourceTime);
              // For SET with multiple parameters
              if (event instanceof DdlParserListener.TableCreatedEvent) {
                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                        SchemaChangeEvent.SchemaChangeEventType.CREATE, snapshot);
              }
              else if (event instanceof DdlParserListener.TableAlteredEvent || event instanceof DdlParserListener.TableIndexCreatedEvent || event instanceof DdlParserListener.TableIndexDroppedEvent) {
                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                        SchemaChangeEvent.SchemaChangeEventType.ALTER, snapshot);
              }
              else if (event instanceof DdlParserListener.TableDroppedEvent) {
                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                        SchemaChangeEvent.SchemaChangeEventType.DROP, snapshot);
              }
              else if (event instanceof DdlParserListener.SetVariableEvent) {
                // SET statement with multiple variable emits event for each variable. We want to emit only
                // one change event
                final DdlParserListener.SetVariableEvent varEvent = (DdlParserListener.SetVariableEvent) event;
                if (varEvent.order() == 0) {
                  emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event,
                          tableId, SchemaChangeEvent.SchemaChangeEventType.DATABASE, snapshot);
                }
              }
              else {
                emitChangeEvent(partition, offset, schemaChangeEvents, sanitizedDbName, event, tableId,
                        SchemaChangeEvent.SchemaChangeEventType.DATABASE, snapshot);
              }
            });
          }
        });
      }
      else {
        offset.databaseEvent(databaseName, sourceTime);
        schemaChangeEvents
                .add(SchemaChangeEvent.ofDatabase(partition, offset, databaseName, ddlStatements, snapshot));
      }
    }
    else {
      LOGGER.debug("Changes for DDL '{}' were filtered and not recorded in database history", ddlStatements);
    }
    return schemaChangeEvents;
  }

}
