package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.junit.Test;

public class TableCodeCTest {
    @Test
    public void decodeTests() {
        byte[] code =
                new byte[] {
                    -128, 0, 4, 0, 0, 0, 1, 2, 3, 4, 10, 0, 14, 0, 31, 0, 33, 0, 65, 65, 71, 68, 80,
                    51, 48, 55, 48, 52, 48, 48, 48, 50, 49, 56, 58, 48, 48, 58, 48, 48, 45, 50, 49,
                    58, 48, 48, 58, 48, 48, 48, 48
                };
    }
}
