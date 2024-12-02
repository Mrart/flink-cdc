package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

import java.io.Serializable;

public class CDCEventEmitter extends RelationalChangeRecordEmitter<TiDBPartition> {
    private final Envelope.Operation operation;
    private final Object[] before;
    private final Object[] after;

    public CDCEventEmitter(
            TiDBPartition partition,
            OffsetContext offsetContext,
            Clock clock,
            Envelope.Operation operation,
            Serializable[] before,
            Serializable[] after) {
        super(partition, offsetContext, clock);
        this.operation = operation;
        this.before = before;
        this.after = after;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return before;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return after;
    }

    @Override
    public Envelope.Operation getOperation() {
        return operation;
    }
}
