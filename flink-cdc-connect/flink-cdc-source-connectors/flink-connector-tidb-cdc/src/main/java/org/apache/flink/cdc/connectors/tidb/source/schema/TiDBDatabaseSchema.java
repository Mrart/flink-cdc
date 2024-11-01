package org.apache.flink.cdc.connectors.tidb.source.schema;

import io.debezium.relational.Key;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.TopicSelector;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBValueConverters;

/** OceanBase database schema. */
public class TiDBDatabaseSchema extends RelationalDatabaseSchema {
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
  }
}
