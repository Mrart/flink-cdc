package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;

import io.debezium.connector.common.CdcSourceTaskContext;

public class TiDBTaskContext extends CdcSourceTaskContext {
    private final TiDBDatabaseSchema schema;

    public TiDBTaskContext(TiDBConnectorConfig config, TiDBDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
        this.schema = schema;
    }

    public TiDBDatabaseSchema getSchema() {
        return schema;
    }
}
