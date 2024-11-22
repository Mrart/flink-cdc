package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.SourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;

public class TiDBSourceInfoStructMaker implements SourceInfoStructMaker<TiDBSourceInfo> {
    private final Schema schema;

    public TiDBSourceInfoStructMaker()
    {
        this.schema = SchemaBuilder.struct()
                .field(TiDBSourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(TiDBSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(TiDBSourceInfo.DATABASE_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(TiDBSourceInfo.SCHEMA_NAME_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(TiDBSourceInfo.TRANSACTION_ID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }
    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(TiDBSourceInfo sourceInfo) {
        Struct source = new Struct(schema);
        source.put(TiDBSourceInfo.TABLE_NAME_KEY, sourceInfo.table());
        Instant timestamp = sourceInfo.timestamp();
        source.put(
                TiDBSourceInfo.TIMESTAMP_KEY,
                timestamp != null ? timestamp.toEpochMilli() : 0);
        if (sourceInfo.database() != null) {
            source.put(TiDBSourceInfo.DATABASE_NAME_KEY, sourceInfo.database());
        }
        if (sourceInfo.transactionId() != null) {
            source.put(TiDBSourceInfo.TRANSACTION_ID_KEY, sourceInfo.transactionId());
        }
        return source;
    }
}
