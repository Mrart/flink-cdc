package org.apache.flink.cdc.connectors.tidb.source.splitter;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkRange;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.table.types.DataType;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TiDBChunkSplitter extends JdbcSourceChunkSplitter {
    public TiDBChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        super(sourceConfig, dialect);
    }

    @Override
    protected Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        return TiDBUtils.queryNextChunkMax(
                jdbc, tableId, splitColumn.name(), chunkSize, includedLowerBound);
    }

    @Override
    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        return TiDBUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    @Override
    protected DataType fromDbzColumn(Column splitColumn) {
        return TiDBUtils.fromDbzColumn(splitColumn);
    }

    @VisibleForTesting
    public List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize) {
        //    LOG.info(
        //            "Use evenly-sized chunk optimization for table {}, the approximate row count
        // is {}, the chunk size is {}, the dynamic chunk size is {}",
        //            tableId,
        //            approximateRowCnt,
        //            chunkSize,
        //            dynamicChunkSize);
        if (approximateRowCnt <= chunkSize) {
            // there is no more than one chunk, return full table as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = ObjectUtils.plus(min, dynamicChunkSize);
        while (ObjectUtils.compare(chunkEnd, max) <= 0) {
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            chunkStart = chunkEnd;
            try {
                chunkEnd = ObjectUtils.plus(chunkEnd, dynamicChunkSize);
            } catch (ArithmeticException e) {
                // Stop chunk split to avoid dead loop when number overflows.
                break;
            }
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }
}
