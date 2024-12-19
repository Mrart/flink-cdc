package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import org.apache.flink.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

public class TiDBSourceBuilder<T> {
    private final TiDBSourceConfigFactory configFactory = new TiDBSourceConfigFactory();
    private CDCEventOffsetFactory offsetFactory;
    private DebeziumDeserializationSchema<T> deserializer;
    private TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
    private TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
    private TiDBDialect dialect;

    private TiDBSourceBuilder() {}

    public TiDBSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        this.configFactory.startupOptions(startupOptions);
        return this;
    }

    public TiDBSourceBuilder<T> hostname(String hostname) {
        this.configFactory.hostname(hostname);
        return this;
    }

    public TiDBSourceBuilder<T> port(int port) {
        this.configFactory.port(port);
        return this;
    }

    public TiDBSourceBuilder<T> driverClassName(String driverClassName) {
        this.configFactory.driverClassName(driverClassName);
        return this;
    }

    public TiDBSourceBuilder<T> databaseList(String... databaseList) {
        this.configFactory.databaseList(databaseList);
        return this;
    }

    public TiDBSourceBuilder<T> tableList(String... tableList) {
        this.configFactory.tableList(tableList);
        return this;
    }

    public TiDBSourceBuilder<T> username(String username) {
        this.configFactory.username(username);
        return this;
    }

    public TiDBSourceBuilder<T> password(String password) {
        this.configFactory.password(password);
        return this;
    }

    public TiDBSourceBuilder<T> jdbcProperties(Properties properties) {
        this.configFactory.jdbcProperties(properties);
        return this;
    }

    public TiDBSourceBuilder<T> tikvProperties(Properties properties) {
        this.configFactory.tikvProperties(properties);
        return this;
    }

    public TiDBSourceBuilder<T> serverTimeZone(String timeZone) {
        this.configFactory.serverTimeZone(timeZone);
        return this;
    }

    public TiDBSourceBuilder<T> connectTimeout(Duration connectTimeout) {
        this.configFactory.connectTimeout(connectTimeout);
        return this;
    }

    public TiDBSourceBuilder<T> connectionPoolSize(int connectionPoolSize) {
        this.configFactory.connectionPoolSize(connectionPoolSize);
        return this;
    }

    public TiDBSourceBuilder<T> connectMaxRetries(int connectMaxRetries) {
        this.configFactory.connectMaxRetries(connectMaxRetries);
        return this;
    }

    public TiDBSourceBuilder<T> chunkKeyColumn(String chunkKeyColumn) {
        this.configFactory.chunkKeyColumn(chunkKeyColumn);
        return this;
    }

    public TiDBSourceBuilder<T> pdAddresses(String pdAddresses) {
        this.configFactory.pdAddresses(pdAddresses);
        return this;
    }

    public TiDBSourceBuilder<T> hostMapping(String hostMapping) {
        this.configFactory.hostMapping(hostMapping);
        return this;
    }
    /**
     * The split size (number of rows) of table snapshot, captured tables are split into multiple
     * splits when read the snapshot of table.
     */
    public TiDBSourceBuilder<T> splitSize(int splitSize) {
        this.configFactory.splitSize(splitSize);
        return this;
    }

    /** The maximum fetch size for per poll when read table snapshot. */
    public TiDBSourceBuilder<T> fetchSize(int fetchSize) {
        this.configFactory.fetchSize(fetchSize);
        return this;
    }

    public TiDBSourceBuilder<T> splitMetaGroupSize(int splitMetaGroupSize) {
        this.configFactory.splitMetaGroupSize(splitMetaGroupSize);
        return this;
    }

    public TiDBSourceBuilder<T> distributionFactorUpper(double distributionFactorUpper) {
        this.configFactory.distributionFactorUpper(distributionFactorUpper);
        return this;
    }

    /**
     * The lower bound of split key evenly distribution factor, the factor is used to determine
     * whether the table is evenly distribution or not.
     */
    public TiDBSourceBuilder<T> distributionFactorLower(double distributionFactorLower) {
        this.configFactory.distributionFactorLower(distributionFactorLower);
        return this;
    }

    public TiDBSourceBuilder<T> scanNewlyAddedTableEnabled(boolean scanNewlyAddedTableEnabled) {
        this.configFactory.scanNewlyAddedTableEnabled(scanNewlyAddedTableEnabled);
        return this;
    }

    public TiDBSourceBuilder<T> deserializer(DebeziumDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    /** The deserializer used to convert from consumed snapshot event from TiKV. */
    public TiDBSourceBuilder<T> snapshotEventDeserializer(
            TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema) {
        this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
        return this;
    }

    /** The deserializer used to convert from consumed change event from TiKV. */
    public TiDBSourceBuilder<T> changeEventDeserializer(
            TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema) {
        this.changeEventDeserializationSchema = changeEventDeserializationSchema;
        return this;
    }

    public TiDBIncrementalSource<T> build() {
        this.offsetFactory = new CDCEventOffsetFactory();
        this.dialect = new TiDBDialect(configFactory.create(0));
        return new TiDBIncrementalSource<>(
                configFactory, checkNotNull(deserializer), offsetFactory, dialect);
    }

    public static class TiDBIncrementalSource<T> extends JdbcIncrementalSource<T> {
        public TiDBIncrementalSource(
                JdbcSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                CDCEventOffsetFactory offsetFactory,
                TiDBDialect dataSourceDialect) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
        }
        //
        //    @Override
        //    public TiDBSourceEnumerator createEnumerator(
        //        SplitEnumeratorContext<SourceSplitBase> enumContext) {
        //      final SplitAssigner splitAssigner;
        //      TiDBSourceConfig sourceConfig = (TiDBSourceConfig) configFactory.create(0);
        //      if (!sourceConfig.getStartupOptions().isStreamOnly()) {
        //        try {
        //          final List<TableId> remainingTables =
        //              dataSourceDialect.discoverDataCollections(sourceConfig);
        //          boolean isTableIdCaseSensitive =
        //              dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
        //          splitAssigner =
        //              new HybridSplitAssigner<>(
        //                  sourceConfig,
        //                  enumContext.currentParallelism(),
        //                  remainingTables,
        //                  isTableIdCaseSensitive,
        //                  dataSourceDialect,
        //                  offsetFactory);
        //        } catch (Exception e) {
        //          throw new FlinkRuntimeException("Failed to discover captured tables for
        // enumerator",
        // e);
        //        }
        //      } else {
        //        splitAssigner = new StreamSplitAssigner(sourceConfig, dataSourceDialect,
        // offsetFactory);
        //      }
        //
        //      return new TiDBSourceEnumerator(
        //          enumContext,
        //          sourceConfig,
        //          splitAssigner,
        //          (TiDBDialect) dataSourceDialect,
        //          this.getBoundedness());
        //    }
        //
        //    @Override
        //    public TiDBSourceEnumerator restoreEnumerator(
        //        SplitEnumeratorContext<SourceSplitBase> enumContext, PendingSplitsState
        // checkpoint) {
        //      final SplitAssigner splitAssigner;
        //      TiDBSourceConfig sourceConfig = (TiDBSourceConfig) configFactory.create(0);
        //      if (checkpoint instanceof HybridPendingSplitsState) {
        //        splitAssigner =
        //            new HybridSplitAssigner<>(
        //                sourceConfig,
        //                enumContext.currentParallelism(),
        //                (HybridPendingSplitsState) checkpoint,
        //                dataSourceDialect,
        //                offsetFactory);
        //      } else if (checkpoint instanceof StreamPendingSplitsState) {
        //        splitAssigner =
        //            new StreamSplitAssigner(
        //                sourceConfig,
        //                (StreamPendingSplitsState) checkpoint,
        //                dataSourceDialect,
        //                offsetFactory);
        //      } else {
        //        throw new UnsupportedOperationException(
        //            "Unsupported restored PendingSplitsState: " + checkpoint);
        //      }
        //
        //      return new TiDBSourceEnumerator(
        //          enumContext,
        //          sourceConfig,
        //          splitAssigner,
        //          (TiDBDialect) dataSourceDialect,
        //          getBoundedness());
        //    }

        //    @Override
        //    public TiDBSourceReader createReader(SourceReaderContext readerContext) throws
        // Exception {
        //      TiDBSourceConfig sourceConfig =
        //          (TiDBSourceConfig) configFactory.create(readerContext.getIndexOfSubtask());
        //      FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementsQueue =
        //          new FutureCompletingBlockingQueue<>();
        //
        //      final SourceReaderMetrics sourceReaderMetrics =
        //          new SourceReaderMetrics(readerContext.metricGroup());
        //
        //      sourceReaderMetrics.registerMetrics();
        //      IncrementalSourceReaderContext incrementalSourceReaderContext =
        //          new IncrementalSourceReaderContext(readerContext);
        //      Supplier<IncrementalSourceSplitReader<JdbcSourceConfig>> splitReaderSupplier =
        //          () ->
        //              new IncrementalSourceSplitReader<>(
        //                  readerContext.getIndexOfSubtask(),
        //                  dataSourceDialect,
        //                  sourceConfig,
        //                  incrementalSourceReaderContext,
        //                  snapshotHooks);
        //      return new TiDBSourceReader(
        //          elementsQueue,
        //          splitReaderSupplier,
        //          createRecordEmitter(sourceConfig, sourceReaderMetrics),
        //          readerContext.getConfiguration(),
        //          incrementalSourceReaderContext,
        //          sourceConfig,
        //          sourceSplitSerializer,
        //          dataSourceDialect);
        //    }

        public static <T> TiDBSourceBuilder<T> builder() {
            return new TiDBSourceBuilder<>();
        }
    }
}
