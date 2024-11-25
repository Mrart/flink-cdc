package io.debezium.connector.tidb;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;

public class TidbTaskContext extends CdcSourceTaskContext {

    private final TiDBDatabaseSchema schema;
    private final TopicSelector<TableId> topicSelector;

    public TidbTaskContext(TiDBConnectorConfig config, TiDBDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
        this.schema = schema;
        topicSelector = TidbTopicSelector.defaultSelector(config);
    }

    public TiDBDatabaseSchema getSchema() {
        return schema;
    }


    public TopicSelector<TableId> getTopicSelector() {
        return topicSelector;
    }
}