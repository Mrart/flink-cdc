package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.tidb.source.TiDBSourceBuilder;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.MetadataConverter;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.tikv.common.TiConfiguration;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class TiDBTableSource implements ScanTableSource, SupportsReadingMetadata {

    private final ResolvedSchema physicalSchema;

    private final StartupOptions startupOptions;
    private final String tableList;
    private final String tableName;
    private final Duration connectTimeout;
    private final String jdbcDriver;
    private final String serverTimeZone;

    private final String pdAddresses;
    private final String hostMapping;

    private final int port;
    private final String hostName;
    private final String database;
    private final String username;
    private final String password;
    private final Duration heartbeatInterval;

    //  incremental snapshot options
    private final int splitSize;
    private final int splitMetaGroupSize;
    private final int fetchSize;
    private final int connectionPoolSize;
    private final int connectMaxRetries;
    private final double distributionFactorUpper;
    private final double distributionFactorLower;
    private final String chunkKeyColumn;

    private final Properties jdbcProperties;
    private final Map<String, String> options;
    private final boolean enableParallelRead;


    /** Data type that describes the final output of the source. */
    protected DataType producedDataType;

    /** Metadata that is appended at the end of a physical source row. */
    protected List<String> metadataKeys;

    public TiDBTableSource(
            ResolvedSchema physicalSchema,
            int port,
            String hostName,
            String database,
            String tableName,
            String tableList,
            String username,
            String password,
            String serverTimeZone,
            Properties jdbcProperties,
            boolean enableParallelRead,
            Duration heartbeatInterval,
            String pdAddresses,
            String hostMapping,
            Duration connectTimeout,
            Map<String, String> options,
            int splitSize,
            int splitMetaGroupSize,
            int fetchSize,
            int connectMaxRetries,
            int connectionPoolSize,
            double distributionFactorUpper,
            double distributionFactorLower,
            @Nullable String chunkKeyColumn,
            String jdbcDriver,
            StartupOptions startupOptions) {
        this.physicalSchema = physicalSchema;
        this.database = checkNotNull(database);
        this.tableName = checkNotNull(tableName);
        this.pdAddresses = checkNotNull(pdAddresses);
        this.port = port;
        this.username = username;
        this.password = password;
        this.serverTimeZone = serverTimeZone;
        this.jdbcProperties = jdbcProperties;
        this.hostName = hostName;
        this.options = options;

        //  incremental snapshot options
        this.enableParallelRead = enableParallelRead;
        this.splitSize = splitSize;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.fetchSize = fetchSize;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.distributionFactorUpper = distributionFactorUpper;
        this.distributionFactorLower = distributionFactorLower;
        this.chunkKeyColumn = chunkKeyColumn;

        this.heartbeatInterval = heartbeatInterval;
        this.jdbcDriver = jdbcDriver;
        this.connectTimeout = connectTimeout;
        this.tableList = tableList;
        this.hostMapping = hostMapping;
        this.startupOptions = startupOptions;
        this.producedDataType = physicalSchema.toPhysicalRowDataType();
        this.metadataKeys = Collections.emptyList();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        // TIDB source  builder
        final TiConfiguration tiConf =
                TiDBSourceOptions.getTiConfiguration(pdAddresses, hostMapping, options);

        RowType physicalDataType =
                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();

        TypeInformation<RowData> typeInfo = scanContext.createTypeInformation(producedDataType);
        MetadataConverter[] metadataConverters = getMetadataConverters();

        //        RowDataTiKVSnapshotEventDeserializationSchema snapshotEventDeserializationSchema =
        //                new RowDataTiKVSnapshotEventDeserializationSchema(
        //                        tiConf,
        //                        database,
        //                        tableName,
        //                        typeInfo,
        //                        metadataConverters,
        //                        physicalDataType);
        //
        //        RowDataTiKVChangeEventDeserializationSchema changeEventDeserializationSchema =
        //                new RowDataTiKVChangeEventDeserializationSchema(
        //                        tiConf,
        //                        database,
        //                        tableName,
        //                        typeInfo,
        //                        metadataConverters,
        //                        physicalDataType);

        //        RowType physicalDataType =
        //                (RowType) physicalSchema.toPhysicalRowDataType().getLogicalType();
        //        MetadataConverter[] metadataConverters = getMetadataConverters();
        //        TypeInformation<RowData> resultTypeInfo =
        // scanContext.createTypeInformation(producedDataType);
        //
        //
        //        //TidbDeserializationConverterFactory   metadataConverters
        //        DebeziumDeserializationSchema<RowData> deserializer =
        //                RowDataDebeziumDeserializeSchema.newBuilder()
        //                        .setPhysicalRowType(physicalDataType)
        //                        .setServerTimeZone(serverTimeZone == null
        //                                ? ZoneId.systemDefault()
        //                                : ZoneId.of(serverTimeZone))
        //                        .setUserDefinedConverterFactory(
        //                                TidbDeserializationConverterFactory.instance())
        //                        .build();
        DebeziumDeserializationSchema<RowData> deserializer =
                RowDataDebeziumDeserializeSchema.newBuilder()
                        .setPhysicalRowType(physicalDataType)
                        .setMetadataConverters(metadataConverters)
                        .setResultTypeInfo(typeInfo)
                        .setServerTimeZone(
                                serverTimeZone == null
                                        ? ZoneId.systemDefault()
                                        : ZoneId.of(serverTimeZone))
                        .setUserDefinedConverterFactory(
                                TidbDeserializationConverterFactory.instance())
                        .build();
        JdbcIncrementalSource<RowData> parallelSource =
                TiDBSourceBuilder.TiDBIncrementalSource.<RowData>builder()
                        .hostname(hostName)
                        .port(port)
                        .databaseList(database)
                        .tableList(database + "\\." + tableName)
                        .username(username)
                        .password(password)
                        .serverTimeZone(serverTimeZone.toString())
                        .splitSize(splitSize)
                        .splitMetaGroupSize(splitMetaGroupSize)
                        .distributionFactorUpper(distributionFactorUpper)
                        .distributionFactorLower(distributionFactorLower)
                        .fetchSize(fetchSize)
                        .connectTimeout(connectTimeout)
                        .connectionPoolSize(connectionPoolSize)
                        .chunkKeyColumn(chunkKeyColumn)
                        .driverClassName(jdbcDriver)
                        .connectMaxRetries(connectMaxRetries)
                        .jdbcProperties(jdbcProperties)
                        .startupOptions(startupOptions)
                        .deserializer(deserializer)
                        //
                        // .snapshotEventDeserializer(snapshotEventDeserializationSchema)
                        //                .changeEventDeserializer(changeEventDeserializationSchema)
                        .build();
        // todo  JdbcIncrementalSource<RowData> parallelSource =
        //                     TiDBSourceBuilder.TiDBIncrementalSource.builder()
        return SourceProvider.of(parallelSource);
    }

    @Override
    public DynamicTableSource copy() {
        TiDBTableSource source =
                new TiDBTableSource(
                        physicalSchema,
                        port,
                        hostName,
                        database,
                        tableName,
                        tableList,
                        username,
                        password,
                        serverTimeZone,
                        jdbcProperties,
                        enableParallelRead,
                        heartbeatInterval,
                        pdAddresses,
                        hostMapping,
                        connectTimeout,
                        options,
                        splitSize,
                        splitMetaGroupSize,
                        fetchSize,
                        connectMaxRetries,
                        connectionPoolSize,
                        distributionFactorUpper,
                        distributionFactorLower,
                        chunkKeyColumn,
                        jdbcDriver,
                        startupOptions);
        source.producedDataType = producedDataType;
        source.metadataKeys = metadataKeys;

        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TiDBTableSource that = (TiDBTableSource) o;
        return port == that.port
                && Objects.equals(physicalSchema, that.physicalSchema)
                && Objects.equals(hostName, that.hostName)
                && Objects.equals(database, that.database)
                && Objects.equals(tableName, that.tableName)
                && Objects.equals(tableList, that.tableList)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password)
                && Objects.equals(serverTimeZone, that.serverTimeZone)
                && Objects.equals(jdbcProperties, that.jdbcProperties)
                && Objects.equals(enableParallelRead, that.enableParallelRead)
                && Objects.equals(heartbeatInterval, that.heartbeatInterval)
                && Objects.equals(pdAddresses, that.pdAddresses)
                && Objects.equals(hostMapping, that.hostMapping)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(jdbcDriver, that.jdbcDriver)
                && Objects.equals(startupOptions, that.startupOptions)
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(metadataKeys, that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalSchema,
                port,
                hostName,
                database,
                tableName,
                tableList,
                username,
                password,
                serverTimeZone,
                jdbcProperties,
                enableParallelRead,
                heartbeatInterval,
                pdAddresses,
                hostMapping,
                connectTimeout,
                options,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                chunkKeyColumn,
                jdbcDriver,
                startupOptions);
    }

    @Override
    public String asSummaryString() {
        return "TiDB-CDC";
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Stream.of(TiKVReadableMetadata.createTiKVReadableMetadata(database, tableName))
                .collect(
                        Collectors.toMap(
                                TiKVReadableMetadata::getKey, TiKVReadableMetadata::getDataType));
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        this.metadataKeys = metadataKeys;
        this.producedDataType = producedDataType;
    }

    // TiKVMetadataConverter to  MetadataConverter
    private MetadataConverter[] getMetadataConverters() {
        if (metadataKeys.isEmpty()) {
            return new MetadataConverter[0];
        }

        return metadataKeys.stream()
                .map(
                        key ->
                                Stream.of(
                                                TiKVReadableMetadata.createTiKVReadableMetadata(
                                                        database, tableName))
                                        .filter(m -> m.getKey().equals(key))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new))
                .map(TiKVReadableMetadata::getConverter)
                .toArray(MetadataConverter[]::new);
    }
}
