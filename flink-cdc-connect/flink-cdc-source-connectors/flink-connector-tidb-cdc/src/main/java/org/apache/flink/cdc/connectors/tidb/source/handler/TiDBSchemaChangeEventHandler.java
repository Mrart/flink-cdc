package org.apache.flink.cdc.connectors.tidb.source.handler;

import io.debezium.schema.SchemaChangeEvent;
import org.apache.flink.cdc.connectors.base.relational.handler.SchemaChangeEventHandler;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;
import java.util.Map;

public class TiDBSchemaChangeEventHandler implements SchemaChangeEventHandler {

    @Override
    public Map<String, Object> parseSource(SchemaChangeEvent event) {
        return new HashMap<>();
    }
}
