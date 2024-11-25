package org.apache.flink.cdc.connectors.tidb.source.config;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.mysql.MySqlConnector;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.relational.*;
import org.apache.flink.cdc.connectors.tidb.source.offset.TiDBSourceInfoStructMaker;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TiDBConnectorConfig extends  RelationalDatabaseConnectorConfig {
    protected static final String LOGICAL_NAME = "tidb_cdc_connector";
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;
    private final boolean readOnlyConnection = true;
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

    public TiDBConnectorConfig( String compatibleMode, Properties properties) {
        super(Configuration.from(properties),
                LOGICAL_NAME,
                Tables.TableFilter.fromPredicate(
                        tableId ->
                                "mysql".equalsIgnoreCase(compatibleMode)
                                        ? !BUILT_IN_DB_NAMES.contains(tableId.catalog())
                                        : !BUILT_IN_DB_NAMES.contains(tableId.schema())),
                TableId::identifier,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                "mysql".equalsIgnoreCase(compatibleMode)
                        ? ColumnFilterMode.CATALOG
                        : ColumnFilterMode.SCHEMA
        );
    }

    @Override
    public String getContextName() {
        return "TiDB";
    }

    @Override
    public String getConnectorName() {
        return "TiDB";
    }

    public String databaseName() {
        return getConfig().getString(DATABASE_NAME);
    }


    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new TiDBSourceInfoStructMaker();
    }

    public static final Field SERVER_NAME = RelationalDatabaseConnectorConfig.SERVER_NAME
            .withValidation(CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public boolean isReadOnlyConnection() {
        return readOnlyConnection;
    }

}
