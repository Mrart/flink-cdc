package org.apache.flink.cdc.connectors.tidb.source.handler;

import org.apache.flink.cdc.connectors.base.relational.handler.SchemaChangeEventHandler;

import io.debezium.schema.SchemaChangeEvent;

import java.util.HashMap;
import java.util.Map;

public class TiDBSchemaChangeEventHandler implements SchemaChangeEventHandler {

    @Override
    public Map<String, Object> parseSource(SchemaChangeEvent event) {
        return new HashMap<>();
    }
}
