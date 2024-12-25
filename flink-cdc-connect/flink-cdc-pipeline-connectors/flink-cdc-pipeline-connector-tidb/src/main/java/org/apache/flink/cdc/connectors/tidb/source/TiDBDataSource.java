package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitState;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceReaderMetrics;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetFactory;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

public class TiDBDataSource implements DataSource {

    private final TiDBSourceConfig tiDBSourceConfig;
    private final TiDBSourceConfigFactory tiDBSourceConfigFactory;

    public TiDBDataSource(
            TiDBSourceConfig tiDBSourceConfig, TiDBSourceConfigFactory tiDBSourceConfigFactory) {
        this.tiDBSourceConfig = tiDBSourceConfigFactory.create(0);
        this.tiDBSourceConfigFactory = tiDBSourceConfigFactory;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        TiDBEventDeserializer tiDBEventDeserializer =
                new TiDBEventDeserializer(
                        DebeziumChangelogMode.ALL, tiDBSourceConfig.isIncludeSchemaChanges());
        //        new TiDBSource<>()
        return null;
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return null;
    }

    public static class TiDBPipelineSOurce<T> extends TiDBSourceBuilder.TiDBIncrementalSource<T> {
        private final TiDBSourceConfig tiDBSourceConfig;
        private final TiDBDialect tiDBDialect;

        public TiDBPipelineSOurce(
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
            return new Tidb<>(
                    deserializationSchema,
                    sourceReaderMetrics,
                    this.sourceConfig,
                    offsetFactory,
                    this.dataSourceDialect);
            return null;
        }
    }
}
