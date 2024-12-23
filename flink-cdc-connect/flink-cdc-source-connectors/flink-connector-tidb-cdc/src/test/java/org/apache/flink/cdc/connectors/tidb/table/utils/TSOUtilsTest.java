package org.apache.flink.cdc.connectors.tidb.table.utils;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;

/** test for unix timestamp */
public class TSOUtilsTest {
    private final long tso = 440395628544000000L;
    private final long uTimestamp = 1679976000000L;

    @Test
    public void TSOToTimeStampTest() {
        long uTimeStamp = TSOUtils.TSOToTimeStamp(tso);
        Assert.assertEquals(uTimeStamp, uTimestamp);
    }

    @Test
    public void InstantToTSOTest() {
        Instant instant = Instant.ofEpochMilli(uTimestamp);
        long toTSO = TSOUtils.InstantToTSO(instant);
        Assert.assertEquals(toTSO, tso);
    }
}
