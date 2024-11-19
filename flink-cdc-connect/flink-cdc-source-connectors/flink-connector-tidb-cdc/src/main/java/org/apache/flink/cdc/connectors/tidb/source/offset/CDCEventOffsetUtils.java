package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.pipeline.spi.OffsetContext;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CDCEventOffsetUtils {
    public static CDCEventOffsetContext getCDCEventOffsetContext(
            OffsetContext.Loader loader, Offset offset) {
        Map<String, String> offsetStrMap =
                Objects.requireNonNull(offset, "offset is null for the sourceSplitBase")
                        .getOffset();
        // all the keys happen to be long type for PostgresOffsetContext.Loader.load
        Map<String, Object> offsetMap = new HashMap<>();
        for (String key : offsetStrMap.keySet()) {
            String value = offsetStrMap.get(key);
            if (value != null) {
                offsetMap.put(key, Long.parseLong(value));
            }
        }
        return (CDCEventOffsetContext) loader.load(offsetMap);
    }
}
