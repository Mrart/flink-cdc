package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetFactory;
import org.apache.flink.cdc.connectors.tidb.source.reader.TiDBPipelineRecordEmitter;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

/** A {@link DataSource} for Postgres cdc connector. */
@Internal
public class TiDBDataSource implements DataSource {

    private final TiDBSourceConfig tiDBSourceConfig;
    private final TiDBSourceConfigFactory tiDBSourceConfigFactory;

    public TiDBDataSource(TiDBSourceConfigFactory tiDBSourceConfigFactory) {
        this.tiDBSourceConfig = tiDBSourceConfigFactory.create(0);
        this.tiDBSourceConfigFactory = tiDBSourceConfigFactory;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        TiDBEventDeserializer tiDBEventDeserializer =
                new TiDBEventDeserializer(
                        DebeziumChangelogMode.ALL, tiDBSourceConfig.isIncludeSchemaChanges());
        CDCEventOffsetFactory cdcEventOffsetFactory = new CDCEventOffsetFactory();
        TiDBDialect tiDBDialect = new TiDBDialect(tiDBSourceConfig);
        TiDBSourceBuilder.TiDBIncrementalSource<Event> tiDBPipelineSource =
                new TiDBPipelineSource<>(
                        tiDBSourceConfigFactory,
                        tiDBEventDeserializer,
                        cdcEventOffsetFactory,
                        tiDBDialect,
                        tiDBSourceConfig);
        return FlinkSourceProvider.of(tiDBPipelineSource);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new TiDBMetadataAccessor(tiDBSourceConfig);
    }

    @VisibleForTesting
    public TiDBSourceConfig gettiDBSourceConfig() {
        return tiDBSourceConfig;
    }

    public static class TiDBPipelineSource<T> extends TiDBSourceBuilder.TiDBIncrementalSource<T> {
        private final TiDBSourceConfig tiDBSourceConfig;
        private final TiDBDialect tiDBDialect;

        public TiDBPipelineSource(
                TiDBSourceConfigFactory configFactory,
                DebeziumDeserializationSchema<T> deserializationSchema,
                CDCEventOffsetFactory offsetFactory,
                TiDBDialect dataSourceDialect,
                TiDBSourceConfig sourceConfig) {
            super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
            this.tiDBSourceConfig = sourceConfig;
            this.tiDBDialect = dataSourceDialect;
        }

        @Override
        protected RecordEmitter<SourceRecords, T, SourceSplitState> createRecordEmitter(
                SourceConfig sourceConfig, SourceReaderMetrics sourceReaderMetrics) {
            return new TiDBPipelineRecordEmitter<>(
                    deserializationSchema,
                    sourceReaderMetrics,
                    this.tiDBSourceConfig,
                    offsetFactory,
                    this.tiDBDialect);
        }
    }
}
