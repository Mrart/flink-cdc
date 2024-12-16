package org.apache.flink.cdc.connectors.tidb.source.config;

import io.debezium.config.EnumeratedValue;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
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

import java.math.BigDecimal;
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

    public static final Field BIGINT_UNSIGNED_HANDLING_MODE = Field.create("bigint.unsigned.handling.mode")
            .withDisplayName("BIGINT UNSIGNED Handling")
            .withEnum(TiDBConnectorConfig.BigIntUnsignedHandlingMode.class, TiDBConnectorConfig.BigIntUnsignedHandlingMode.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 27))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specify how BIGINT UNSIGNED columns should be represented in change events, including:"
                    + "'precise' uses java.math.BigDecimal to represent values, which are encoded in the change events using a binary representation and Kafka Connect's 'org.apache.kafka.connect.data.Decimal' type; "
                    + "'long' (the default) represents values using Java's 'long', which may not offer the precision but will be far easier to use in consumers.");

    public static final Field ENABLE_TIME_ADJUSTER = Field.create("enable.time.adjuster")
            .withDisplayName("Enable Time Adjuster")
            .withType(ConfigDef.Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 22))
            .withDefault(true)
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription(
                    "MySQL allows user to insert year value as either 2-digit or 4-digit. In case of two digit the value is automatically mapped into 1970 - 2069." +
                            "false - delegates the implicit conversion to the database" +
                            "true - (the default) Debezium makes the conversion");

    public static enum BigIntUnsignedHandlingMode implements EnumeratedValue {
        /**
         * Represent {@code BIGINT UNSIGNED} values as precise {@link BigDecimal} values, which are
         * represented in change events in a binary form. This is precise but difficult to use.
         */
        PRECISE("precise"),

        /**
         * Represent {@code BIGINT UNSIGNED} values as precise {@code long} values. This may be less precise
         * but is far easier to use.
         */
        LONG("long");

        private final String value;

        private BigIntUnsignedHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public JdbcValueConverters.BigIntUnsignedMode asBigIntUnsignedMode() {
            switch (this) {
                case LONG:
                    return JdbcValueConverters.BigIntUnsignedMode.LONG;
                case PRECISE:
                default:
                    return JdbcValueConverters.BigIntUnsignedMode.PRECISE;
            }
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static TiDBConnectorConfig.BigIntUnsignedHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (TiDBConnectorConfig.BigIntUnsignedHandlingMode option : TiDBConnectorConfig.BigIntUnsignedHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static TiDBConnectorConfig.BigIntUnsignedHandlingMode parse(String value, String defaultValue) {
            TiDBConnectorConfig.BigIntUnsignedHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

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
