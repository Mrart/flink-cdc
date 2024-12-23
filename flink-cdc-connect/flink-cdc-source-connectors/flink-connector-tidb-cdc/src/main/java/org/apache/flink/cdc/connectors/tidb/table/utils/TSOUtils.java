package org.apache.flink.cdc.connectors.tidb.table.utils;

import java.time.Instant;

public class TSOUtils {
    private static final int PHYSICAL_SHIFT_BITS = 18;

    // Tidb TSO trans to 13 bit unix timestamp
    public static long TSOToTimeStamp(long tso) {
        return tso >> PHYSICAL_SHIFT_BITS;
    }

    // instant to TSO.
    public static long InstantToTSO(Instant instant) {
        return (instant.toEpochMilli() << PHYSICAL_SHIFT_BITS) + 0;
    }

    // instant to TSO.
    public static long TimestampToTSO(long timestamp) {
        return (timestamp << PHYSICAL_SHIFT_BITS) + 0;
    }
}
