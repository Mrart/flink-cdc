package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.tidb.source.offset.TiDBSourceInfoStructMaker;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TiDBConnectorConfig extends RelationalDatabaseConnectorConfig {
    protected static final String LOGICAL_NAME = "tidb_cdc_connector";
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;
    private final boolean readOnlyConnection = true;
    protected static final List<String> BUILT_IN_DB_NAMES =
            Collections.unmodifiableList(
                    Arrays.asList("information_schema", "mysql", "tidb", "LBACSYS", "ORAAUDITOR"));
    private final TiDBSourceConfig sourceConfig;

    public static final Field READ_ONLY_CONNECTION =
            Field.create("read.only")
                    .withDisplayName("Read only connection")
                    .withType(ConfigDef.Type.BOOLEAN)
                    .withDefault(false)
                    .withWidth(ConfigDef.Width.SHORT)
                    .withImportance(ConfigDef.Importance.LOW)
                    .withDescription(
                            "Switched connector to use alternative methods to deliver signals to Debezium instead of writing to signaling table");

    //  public TiDBConnectorConfig(String compatibleMode, Properties properties) {
    //    super(
    //        Configuration.from(properties),
    //        LOGICAL_NAME,
    //        Tables.TableFilter.fromPredicate(
    //            tableId ->
    //                "mysql".equalsIgnoreCase(compatibleMode)
    //                    ? !BUILT_IN_DB_NAMES.contains(tableId.catalog())
    //                    : !BUILT_IN_DB_NAMES.contains(tableId.schema())),
    //        TableId::identifier,
    //        DEFAULT_SNAPSHOT_FETCH_SIZE,
    //        "mysql".equalsIgnoreCase(compatibleMode)
    //            ? ColumnFilterMode.CATALOG
    //            : ColumnFilterMode.SCHEMA);
    //  }

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

    public TiDBConnectorConfig(TiDBSourceConfig sourceConfig) {
        super(
                Configuration.from(sourceConfig.getDbzProperties()),
                LOGICAL_NAME,
                Tables.TableFilter.fromPredicate(
                        tableId ->
                                "mysql".equalsIgnoreCase(sourceConfig.getCompatibleMode())
                                        ? !BUILT_IN_DB_NAMES.contains(tableId.catalog())
                                        : !BUILT_IN_DB_NAMES.contains(tableId.schema())),
                TableId::identifier,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                "mysql".equalsIgnoreCase(sourceConfig.getCompatibleMode())
                        ? ColumnFilterMode.CATALOG
                        : ColumnFilterMode.SCHEMA);
        this.sourceConfig = sourceConfig;
    }

    public TiDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new TiDBSourceInfoStructMaker();
    }

    public static final Field SERVER_NAME =
            RelationalDatabaseConnectorConfig.SERVER_NAME.withValidation(
                    CommonConnectorConfig::validateServerNameIsDifferentFromHistoryTopicName);

    public boolean isReadOnlyConnection() {
        return readOnlyConnection;
    }
}
