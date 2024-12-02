package org.apache.flink.cdc.connectors.tidb.source.connection;

import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class TiDBConnection extends JdbcConnection {
    private static final Logger LOG = LoggerFactory.getLogger(TiDBConnection.class);

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();
    private static final String MYSQL_URL_PATTERN =
            "jdbc:mysql://${hostname}:${port}/?connectTimeout=${connectTimeout}";

    private static final int TYPE_BINARY_FLOAT = 100;
    private static final int TYPE_BINARY_DOUBLE = 101;
    private static final int TYPE_TIMESTAMP_WITH_TIME_ZONE = -101;
    private static final int TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = -102;
    private static final int TYPE_INTERVAL_YEAR_TO_MONTH = -103;
    private static final int TYPE_INTERVAL_DAY_TO_SECOND = -104;
    private static final char quote = '`';

    public TiDBConnection(
            String hostname,
            Integer port,
            String user,
            String password,
            Duration timeout,
            String jdbcDriver,
            Properties jdbcProperties,
            ClassLoader classLoader) {
        super(
                config(hostname, port, user, password, timeout),
                JdbcConnection.patternBasedFactory(
                        formatJdbcUrl(jdbcDriver, jdbcProperties), jdbcDriver, classLoader),
                quote + "",
                quote + "");
    }

    public TiDBConnection(
            JdbcConfiguration config,
            ConnectionFactory connectionFactory,
            String openingQuoteCharacter,
            String closingQuoteCharacter) {
        super(config, connectionFactory, openingQuoteCharacter, closingQuoteCharacter);
    }

    public TiDBConnection(
            JdbcConfiguration config,
            ConnectionFactory connectionFactory,
            Supplier<ClassLoader> classLoaderSupplier,
            String openingQuoteCharacter,
            String closingQuoteCharacter) {
        super(
                config,
                connectionFactory,
                classLoaderSupplier,
                openingQuoteCharacter,
                closingQuoteCharacter);
    }

    protected TiDBConnection(
            JdbcConfiguration config,
            ConnectionFactory connectionFactory,
            Operations initialOperations,
            Supplier<ClassLoader> classLoaderSupplier,
            String openingQuotingChar,
            String closingQuotingChar) {
        super(
                config,
                connectionFactory,
                initialOperations,
                classLoaderSupplier,
                openingQuotingChar,
                closingQuotingChar);
    }

    private static JdbcConfiguration config(
            String hostname, Integer port, String user, String password, Duration timeout) {
        return JdbcConfiguration.create()
                .with("hostname", hostname)
                .with("port", port)
                .with("user", user)
                .with("password", password)
                .with("connectTimeout", timeout == null ? 30000 : timeout.toMillis())
                .build();
    }

    private static String formatJdbcUrl(String jdbcDriver, Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
        if (jdbcProperties != null) {
            combinedProperties.putAll(jdbcProperties);
        }
        StringBuilder jdbcUrlStringBuilder = new StringBuilder(MYSQL_URL_PATTERN);
        combinedProperties.forEach(
                (key, value) -> {
                    jdbcUrlStringBuilder.append("&").append(key).append("=").append(value);
                });
        return jdbcUrlStringBuilder.toString();
    }

    private static Properties initializeDefaultJdbcProperties() {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("useInformationSchema", "true");
        defaultJdbcProperties.setProperty("nullCatalogMeansCurrent", "false");
        defaultJdbcProperties.setProperty("useUnicode", "true");
        defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "convertToNull");
        defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
        defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");
        return defaultJdbcProperties;
    }

    public long getCurrentTimestampS() throws SQLException {
        try {
            long globalTimestamp = getGlobalTimestamp();
            LOG.info("Global timestamp: {}", globalTimestamp);
            return Long.parseLong(String.valueOf(globalTimestamp).substring(0, 10));
        } catch (Exception e) {
            LOG.warn("Failed to get global timestamp, use local timestamp instead");
        }
        return getCurrentTimestamp()
                .orElseThrow(IllegalStateException::new)
                .toInstant()
                .getEpochSecond();
    }

    private long getGlobalTimestamp() throws SQLException {
        return querySingleValue(
                connection(), "SELECT CURRENT_TIMESTAMP FROM DUAL", ps -> {}, rs -> rs.getLong(1));
    }

    @Override
    public Optional<Timestamp> getCurrentTimestamp() throws SQLException {
        return queryAndMap(
                "SELECT LOCALTIMESTAMP FROM DUAL",
                rs -> rs.next() ? Optional.of(rs.getTimestamp(1)) : Optional.empty());
    }

    @Override
    protected String[] supportedTableTypes() {
        return new String[] {"TABLE"};
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        return tableId.toQuotedString(quote);
    }

    public void readSchemaForCapturedTables(
            Tables tables,
            String databaseCatalog,
            String schemaNamePattern,
            Tables.ColumnNameFilter columnFilter,
            boolean removeTablesNotFoundInJdbc,
            Set<TableId> capturedTables)
            throws SQLException {

        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        DatabaseMetaData metadata = connection().getMetaData();
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        for (TableId tableId : capturedTables) {
            try (ResultSet columnMetadata =
                    metadata.getColumns(
                            databaseCatalog, schemaNamePattern, tableId.table(), null)) {
                while (columnMetadata.next()) {
                    // add all whitelisted columns
                    readTableColumn(columnMetadata, tableId, columnFilter)
                            .ifPresent(
                                    column -> {
                                        columnsByTable
                                                .computeIfAbsent(tableId, t -> new ArrayList<>())
                                                .add(column.create());
                                    });
                }
            }
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, null);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    @Override
    protected int resolveNativeType(String typeName) {
        String upperCaseTypeName = typeName.toUpperCase();
        if (upperCaseTypeName.startsWith("JSON")) {
            return Types.VARCHAR;
        }
        if (upperCaseTypeName.startsWith("NCHAR")) {
            return Types.NCHAR;
        }
        if (upperCaseTypeName.startsWith("NVARCHAR2")) {
            return Types.NVARCHAR;
        }
        if (upperCaseTypeName.startsWith("TIMESTAMP")) {
            if (upperCaseTypeName.contains("WITH TIME ZONE")) {
                return TYPE_TIMESTAMP_WITH_TIME_ZONE;
            }
            if (upperCaseTypeName.contains("WITH LOCAL TIME ZONE")) {
                return TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            }
            return Types.TIMESTAMP;
        }
        if (upperCaseTypeName.startsWith("INTERVAL")) {
            if (upperCaseTypeName.contains("TO MONTH")) {
                return TYPE_INTERVAL_YEAR_TO_MONTH;
            }
            if (upperCaseTypeName.contains("TO SECOND")) {
                return TYPE_INTERVAL_DAY_TO_SECOND;
            }
        }
        return Column.UNSET_INT_VALUE;
    }

    public String readSystemVariable(String variable) throws SQLException {
        return querySingleValue(
                connection(),
                "SHOW VARIABLES LIKE ?",
                ps -> ps.setString(1, variable),
                rs -> rs.getString("VALUE"));
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        switch (metadataJdbcType) {
            case TYPE_BINARY_FLOAT:
                return Types.REAL;
            case TYPE_BINARY_DOUBLE:
                return Types.DOUBLE;
            case TYPE_TIMESTAMP_WITH_TIME_ZONE:
            case TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TYPE_INTERVAL_YEAR_TO_MONTH:
            case TYPE_INTERVAL_DAY_TO_SECOND:
                return Types.OTHER;
            default:
                return nativeType == Column.UNSET_INT_VALUE ? metadataJdbcType : nativeType;
        }
    }

    public List<TableId> getTables(String dbPattern, String tbPattern) throws SQLException {
        return listTables(
                db -> Pattern.matches(dbPattern, db),
                tableId -> Pattern.matches(tbPattern, tableId.table()));
    }

    private List<TableId> listTables(
            Predicate<String> databaseFilter, Tables.TableFilter tableFilter) throws SQLException {
        List<TableId> tableIds = new ArrayList<>();
        DatabaseMetaData metaData = connection().getMetaData();
        ResultSet rs = metaData.getCatalogs();
        List<String> dbList = new ArrayList<>();
        while (rs.next()) {
            String db = rs.getString("TABLE_CAT");
            if (databaseFilter.test(db)) {
                dbList.add(db);
            }
        }
        for (String db : dbList) {

            rs = metaData.getTables(db, null, null, supportedTableTypes());
            while (rs.next()) {
                TableId tableId = new TableId(db, null, rs.getString("TABLE_NAME"));
                if (tableFilter.isIncluded(tableId)) {
                    tableIds.add(tableId);
                }
            }
        }
        return tableIds;
    }
}
