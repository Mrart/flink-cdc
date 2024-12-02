package org.apache.flink.cdc.connectors.tidb.source.enumetator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.events.OffsetCommitEvent;

import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigning;
import static org.apache.flink.cdc.connectors.base.source.assigner.AssignerStatus.isNewlyAddedAssigningSnapshotFinished;

public class TiDBSourceEnumerator extends IncrementalSourceEnumerator {
    private final TiDBDialect tiDBDialect;
    private final TiDBSourceConfig sourceConfig;

    private volatile boolean receiveOffsetCommitAck = false;

    public TiDBSourceEnumerator(
            SplitEnumeratorContext<SourceSplitBase> context,
            TiDBSourceConfig sourceConfig,
            SplitAssigner splitAssigner,
            TiDBDialect tiDBDialect,
            Boundedness boundedness) {
        super(context, sourceConfig, splitAssigner, boundedness);
        this.tiDBDialect = tiDBDialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void assignSplits() {
        // if scan newly added table is enable, can not assign new added table's snapshot splits
        // until source reader doesn't commit offset.
        if (sourceConfig.isScanNewlyAddedTableEnabled()
                && streamSplitTaskId != null
                && !receiveOffsetCommitAck
                && isNewlyAddedAssigning(splitAssigner.getAssignerStatus())) {
            // just return here, the reader has been put into readersAwaitingSplit, will be assigned
            // split again later
            return;
        }
        super.assignSplits();
    }

    @Override
    protected void syncWithReaders(int[] subtaskIds, Throwable t) {
        super.syncWithReaders(subtaskIds, t);
        // if scan newly added table is enable, postgres enumerator will send its OffsetCommitEvent
        // to tell reader whether to start offset commit.
        if (!receiveOffsetCommitAck
                && sourceConfig.isScanNewlyAddedTableEnabled()
                && streamSplitTaskId != null) {
            AssignerStatus assignerStatus = splitAssigner.getAssignerStatus();
            context.sendEventToSourceReader(
                    streamSplitTaskId,
                    new OffsetCommitEvent(
                            !isNewlyAddedAssigning(assignerStatus)
                                    && !isNewlyAddedAssigningSnapshotFinished(assignerStatus)));
        }
    }
}
