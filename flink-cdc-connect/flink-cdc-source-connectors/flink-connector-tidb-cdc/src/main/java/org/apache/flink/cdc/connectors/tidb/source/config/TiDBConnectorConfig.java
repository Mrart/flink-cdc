package org.apache.flink.cdc.connectors.tidb.source.config;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Tables;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TiDBConnectorConfig extends MySqlConnectorConfig {
    protected static final String LOGICAL_NAME = "tidb_cdc_connector";
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;
    // todo
    protected static final List<String> BUILT_IN_DB_NAMES =
            Collections.unmodifiableList(
                    Arrays.asList("information_schema", "mysql", "tidb", "LBACSYS", "ORAAUDITOR"));

    public static final Field READ_ONLY_CONNECTION = Field.create("read.only")
            .withDisplayName("Read only connection")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Switched connector to use alternative methods to deliver signals to Debezium instead of writing to signaling table");

    public TiDBConnectorConfig(Configuration config) {
        super(
                config
        );
    }

//    public TiDBConnectorConfigTest(TiDBSourceConfig sourceConfig) {
//        super(
//                Configuration.from(sourceConfig.getDbzProperties()),
//                LOGICAL_NAME,
//                Tables.TableFilter.fromPredicate(
//                        tableId ->
//                                "mysql".equalsIgnoreCase(sourceConfig.getCompatibleMode())
//                                        ? !BUILT_IN_DB_NAMES.contains(tableId.catalog())
//                                        : !BUILT_IN_DB_NAMES.contains(tableId.schema())),
//                TableId::identifier,
//                DEFAULT_SNAPSHOT_FETCH_SIZE,
//                "mysql".equalsIgnoreCase(sourceConfig.getCompatibleMode())
//                        ? ColumnFilterMode.CATALOG
//                        : ColumnFilterMode.SCHEMA);
//        this.sourceConfig = sourceConfig;
//    }

    @Override
    public String getContextName() {
        return "TiDB";
    }

    @Override
    public String getConnectorName() {
        return "TiDB";
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return null;
    }

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

}

