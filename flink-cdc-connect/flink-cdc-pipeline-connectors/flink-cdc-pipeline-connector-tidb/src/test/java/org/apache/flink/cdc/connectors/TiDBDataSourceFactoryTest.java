package org.apache.flink.cdc.connectors;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.factory.TiDBDataSourceFactory;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDataSource;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.tidb.source.TiDBDataSourceOptions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TiDBDataSourceFactory}. */
public class TiDBDataSourceFactoryTest extends TiDBTestBase {
    private static final String databaseName = "inventory";
    private static final String tableName = "products";

    @Test
    public void testCreateSource() {
        initializeTidbTable("inventory_pipeline");

        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), TIDB.getHost());
        options.put(TiDB_PORT.key(), String.valueOf(TIDB.getMappedPort(TIDB_PORT)));
        options.put(USERNAME.key(), TiDBTestBase.TIDB_USER);
        options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), "inventory.products:id;inventory.table2:column2");
        options.put(PASSWORD.key(), TiDBTestBase.TIDB_PASSWORD);
        options.put(TABLE_LIST.key(), this.databaseName + "." + this.tableName);
        options.put(DATABASE_NAME.key(), databaseName);
        options.put(TABLES.key(), databaseName + ".prod\\.*");
        options.put(
                PD_ADDRESSES.key(),
                PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN));
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        TiDBDataSourceFactory tiDBDataSourceFactory = new TiDBDataSourceFactory();
        TiDBDataSource tiDBDataSource =
                (TiDBDataSource) tiDBDataSourceFactory.createDataSource(context);

        assertThat(tiDBDataSource.gettiDBSourceConfig().getTableList())
                .isEqualTo(Arrays.asList(databaseName + ".products"));
    }

        @Test
        public void testNoMatchedTable() {
            initializeTidbTable("inventory_pipeline");

            String tables = this.databaseName + ".test";
            Map<String, String> options = new HashMap<>();
            options.put(HOSTNAME.key(), TIDB.getHost());
            options.put(TiDB_PORT.key(), String.valueOf(TIDB.getMappedPort(TIDB_PORT)));
            options.put(USERNAME.key(), TiDBTestBase.TIDB_USER);
            options.put(PASSWORD.key(), TiDBTestBase.TIDB_PASSWORD);
            options.put(TABLE_LIST.key(), tables);
            options.put(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(), databaseName + ".id");
            options.put(DATABASE_NAME.key(), databaseName);
            options.put(TABLES.key(), databaseName + ".prod\\.*");
            options.put(
                    PD_ADDRESSES.key(),
                    PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN));
            Factory.Context context = new MockContext(Configuration.fromMap(options));

            TiDBDataSourceFactory factory = new TiDBDataSourceFactory();
            assertThatThrownBy(() -> factory.createDataSource(context))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Cannot find any table by the option 'tables' = " +
     tables);
        }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
