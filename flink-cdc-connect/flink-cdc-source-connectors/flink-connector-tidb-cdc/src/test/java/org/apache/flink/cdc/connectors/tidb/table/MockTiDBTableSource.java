package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.util.FlinkRuntimeException;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class MockTiDBTableSource extends TiDBTableSource {
    public MockTiDBTableSource(TiDBTableSource tiDBTableSource) {
        super(
                (ResolvedSchema) get(tiDBTableSource, "physicalSchema"),
                (int) get(tiDBTableSource, "port"),
                (String) get(tiDBTableSource, "hostname"),
                (String) get(tiDBTableSource, "database"),
                //                (String) get(postgreSQLTableSource, "schemaName"),
                (String) get(tiDBTableSource, "tableName"),
                (String) get(tiDBTableSource, "tableList"),
                (String) get(tiDBTableSource, "username"),
                (String) get(tiDBTableSource, "password"),
                (String) get(tiDBTableSource, "serverTimeZone"),
                (Properties) get(tiDBTableSource, "dbzProperties"),
                (Boolean) get(tiDBTableSource, "enableParallelRead"),
                (Duration) get(tiDBTableSource, "heartbeatInterval"),
                (String) get(tiDBTableSource, "pdAddresses"),
                (String) get(tiDBTableSource, "hostMapping"),
                (Duration) get(tiDBTableSource, "connectTimeout"),
                (Map<String, String>) get(tiDBTableSource, "options"),
                (int) get(tiDBTableSource, "splitSize"),
                (int) get(tiDBTableSource, "splitMetaGroupSize"),
                (int) get(tiDBTableSource, "fetchSize"),
                (int) get(tiDBTableSource, "connectMaxRetries"),
                (int) get(tiDBTableSource, "connectionPoolSize"),
                (double) get(tiDBTableSource, "distributionFactorUpper"),
                (double) get(tiDBTableSource, "distributionFactorLower"),
                (String) get(tiDBTableSource, "chunkKeyColumn"),
                (Map<ObjectPath, String>) get(tiDBTableSource, "chunkKeyColumn"),
                (String) get(tiDBTableSource, "jdbcDriver"),
                (StartupOptions) get(tiDBTableSource, "startupOptions"));
    }

    private static Object get(TiDBTableSource tiDBTableSource, String name) {
        try {
            Field field = tiDBTableSource.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(tiDBTableSource);
        } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
