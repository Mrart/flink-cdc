package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.table.connector.source.DynamicTableSource;

public class MockTiDBTableFactory extends TiDBTableSourceFactory {

    public static final String IDENTIFIER = "tidb-cdc-mock";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TiDBTableSource TiDBTableSource = (TiDBTableSource) super.createDynamicTableSource(context);

        return new MockTiDBTableSource(TiDBTableSource);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
