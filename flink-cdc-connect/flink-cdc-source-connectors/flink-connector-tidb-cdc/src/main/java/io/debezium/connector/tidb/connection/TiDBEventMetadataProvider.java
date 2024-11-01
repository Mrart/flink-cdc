package io.debezium.connector.tidb.connection;

import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

public class TiDBEventMetadataProvider implements EventMetadataProvider {
    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return null;
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return null;
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return null;
    }

    @Override
    public String toSummaryString(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        return EventMetadataProvider.super.toSummaryString(source, offset, key, value);
    }
}
