package io.debezium.connector.tidb;

import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

public class TiDBOffsetContext implements OffsetContext {
    @Override
    public Map<String, ?> getOffset() {
        return null;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return null;
    }

    @Override
    public Struct getSourceInfo() {
        return null;
    }

    @Override
    public boolean isSnapshotRunning() {
        return false;
    }

    @Override
    public void markLastSnapshotRecord() {

    }

    @Override
    public void preSnapshotStart() {

    }

    @Override
    public void preSnapshotCompletion() {

    }

    @Override
    public void postSnapshotCompletion() {

    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {

    }

    @Override
    public TransactionContext getTransactionContext() {
        return null;
    }

    @Override
    public void incrementalSnapshotEvents() {
        OffsetContext.super.incrementalSnapshotEvents();
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return OffsetContext.super.getIncrementalSnapshotContext();
    }
}
