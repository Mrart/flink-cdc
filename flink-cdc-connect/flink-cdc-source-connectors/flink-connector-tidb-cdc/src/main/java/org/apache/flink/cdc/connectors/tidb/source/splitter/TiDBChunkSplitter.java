package org.apache.flink.cdc.connectors.tidb.source.splitter;

import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkRange;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.utils.JdbcChunkUtils;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.*;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ROUND_CEILING;
import static org.apache.flink.cdc.connectors.base.utils.ObjectUtils.doubleCompare;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;

public class TiDBChunkSplitter implements ChunkSplitter {
    private TiDBSourceConfig tiDBSourceConfig;
    protected final JdbcDataSourceDialect dialect;
    private static final Logger LOG = LoggerFactory.getLogger(TiDBChunkSplitter.class);

    public TiDBChunkSplitter(TiDBSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
        this.dialect = dialect;
        this.tiDBSourceConfig = sourceConfig;
    }

    /** Generates all snapshot splits (chunks) for the give table path. */
    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) {

        try (JdbcConnection jdbc = dialect.openJdbcConnection(tiDBSourceConfig)) {

            LOG.info("Start splitting table {} into chunks...", tableId);
            long start = System.currentTimeMillis();

            Table table =
                    Objects.requireNonNull(dialect.queryTableSchema(jdbc, tableId)).getTable();
            Column splitColumn = getChunkKeyColumn(table, tiDBSourceConfig.getChunkKeyColumns());
            final List<ChunkRange> chunks;
            try {
                chunks = splitTableIntoChunks(jdbc, tableId, splitColumn);
            } catch (SQLException e) {
                throw new FlinkRuntimeException("Failed to split chunks for table " + tableId, e);
            }

            // convert chunks into splits
            List<SnapshotSplit> splits = new ArrayList<>();
            RowType splitType = getSplitType(splitColumn);
            for (int i = 0; i < chunks.size(); i++) {
                ChunkRange chunk = chunks.get(i);
                SnapshotSplit split =
                        createSnapshotSplit(
                                jdbc,
                                tableId,
                                i,
                                splitType,
                                chunk.getChunkStart(),
                                chunk.getChunkEnd());
                splits.add(split);
            }

            long end = System.currentTimeMillis();
            LOG.info(
                    "Split table {} into {} chunks, time cost: {}ms.",
                    tableId,
                    splits.size(),
                    end - start);
            return splits;
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    String.format("Generate Splits for table %s error", tableId), e);
        }
    }

    public static Column getChunkKeyColumn(Table table, Map<ObjectPath, String> chunkKeyColumns) {
        List<Column> primaryKeys = table.primaryKeyColumns();
        String chunkKeyColumn = findChunkKeyColumn(table.id(), chunkKeyColumns);
        LOG.info("current table : " + table.id() + " chunkkeycolumn is " + chunkKeyColumn);
        if (primaryKeys.isEmpty() && chunkKeyColumn == null) {
            throw new ValidationException(
                    "'scan.incremental.snapshot.chunk.key-column' must be set when the table doesn't have primary keys.");
        }

        List<Column> searchColumns = table.columns();
        if (chunkKeyColumn != null) {
            Optional<Column> targetColumn =
                    searchColumns.stream()
                            .filter(col -> chunkKeyColumn.equals(col.name()))
                            .findFirst();
            if (targetColumn.isPresent()) {
                return targetColumn.get();
            }
            throw new ValidationException(
                    String.format(
                            "Chunk key column '%s' doesn't exist in the columns [%s] of the table %s.",
                            chunkKeyColumn,
                            searchColumns.stream()
                                    .map(Column::name)
                                    .collect(Collectors.joining(",")),
                            table.id()));
        }
        // use the first column of primary key columns as the chunk key column by default
        return primaryKeys.get(0);
    }

    @Nullable
    private static String findChunkKeyColumn(
            TableId tableId, Map<ObjectPath, String> chunkKeyColumns) {
        for (ObjectPath table : chunkKeyColumns.keySet()) {
            Tables.TableFilter filter =
                    createTableFilter(table.getDatabaseName(), table.getFullName());
            if (filter.isIncluded(tableId)) {
                return chunkKeyColumns.get(table);
            }
        }
        return null;
    }

    /** Create a TableFilter by database name and table name. */
    private static Tables.TableFilter createTableFilter(String database, String table) {
        final Selectors.TableSelectionPredicateBuilder eligibleTables =
                Selectors.tableSelector().includeDatabases(database);

        Predicate<TableId> tablePredicate = eligibleTables.includeTables(table).build();

        Predicate<TableId> finalTablePredicate =
                tablePredicate.and(
                        Tables.TableFilter.fromPredicate(MySqlConnectorConfig::isNotBuiltInTable)
                                ::isIncluded);
        return finalTablePredicate::test;
    }

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

    protected Object queryMin(
            JdbcConnection jdbc, TableId tableId, Column splitColumn, Object excludedLowerBound)
            throws SQLException {
        return JdbcChunkUtils.queryMin(
                jdbc,
                jdbc.quotedTableIdString(tableId),
                jdbc.quotedColumnIdString(splitColumn.name()),
                excludedLowerBound);
    }

    protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        return TiDBUtils.queryApproximateRowCnt(jdbc, tableId);
    }

    protected DataType fromDbzColumn(Column splitColumn) {
        return TiDBUtils.fromDbzColumn(splitColumn);
    }

    private RowType getSplitType(Column splitColumn) {
        return (RowType)
                ROW(FIELD(splitColumn.name(), fromDbzColumn(splitColumn))).getLogicalType();
    }

    /**
     * We can use evenly-sized chunks or unevenly-sized chunks when split table into chunks, using
     * evenly-sized chunks which is much efficient, using unevenly-sized chunks which will request
     * many queries and is not efficient.
     */
    private List<ChunkRange> splitTableIntoChunks(
            JdbcConnection jdbc, TableId tableId, Column splitColumn) throws SQLException {
        final Object[] minMax = queryMinMax(jdbc, tableId, splitColumn);
        final Object min = minMax[0];
        final Object max = minMax[1];
        if (min == null || max == null || min.equals(max)) {
            // empty table, or only one row, return full table scan as a chunk
            return Collections.singletonList(ChunkRange.all());
        }

        final int chunkSize = tiDBSourceConfig.getSplitSize();
        final double distributionFactorUpper = tiDBSourceConfig.getDistributionFactorUpper();
        final double distributionFactorLower = tiDBSourceConfig.getDistributionFactorLower();

        if (isEvenlySplitColumn(splitColumn)) {
            long approximateRowCnt = queryApproximateRowCnt(jdbc, tableId);
            double distributionFactor =
                    calculateDistributionFactor(tableId, min, max, approximateRowCnt);

            boolean dataIsEvenlyDistributed =
                    doubleCompare(distributionFactor, distributionFactorLower) >= 0
                            && doubleCompare(distributionFactor, distributionFactorUpper) <= 0;

            if (dataIsEvenlyDistributed) {
                // the minimum dynamic chunk size is at least 1
                final int dynamicChunkSize = Math.max((int) (distributionFactor * chunkSize), 1);
                return splitEvenlySizedChunks(
                        tableId, min, max, approximateRowCnt, chunkSize, dynamicChunkSize);
            } else {
                return splitUnevenlySizedChunks(jdbc, tableId, splitColumn, min, max, chunkSize);
            }
        } else {
            return splitUnevenlySizedChunks(jdbc, tableId, splitColumn, min, max, chunkSize);
        }
    }

    /**
     * Split table into evenly sized chunks based on the numeric min and max value of split column,
     * and tumble chunks in step size.
     */
    public List<ChunkRange> splitEvenlySizedChunks(
            TableId tableId,
            Object min,
            Object max,
            long approximateRowCnt,
            int chunkSize,
            int dynamicChunkSize) {
        LOG.info(
                "Use evenly-sized chunk optimization for table {}, the approximate row count is {}, the chunk size is {}, the dynamic chunk size is {}",
                tableId,
                approximateRowCnt,
                chunkSize,
                dynamicChunkSize);
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

    /** Split table into unevenly sized chunks by continuously calculating next chunk max value. */
    private List<ChunkRange> splitUnevenlySizedChunks(
            JdbcConnection jdbc,
            TableId tableId,
            Column splitColumn,
            Object min,
            Object max,
            int chunkSize)
            throws SQLException {
        LOG.info(
                "Use unevenly-sized chunks for table {}, the chunk size is {}", tableId, chunkSize);
        final List<ChunkRange> splits = new ArrayList<>();
        Object chunkStart = null;
        Object chunkEnd = nextChunkEnd(jdbc, min, tableId, splitColumn, max, chunkSize);
        int count = 0;
        while (chunkEnd != null && isChunkEndLeMax(chunkEnd, max, splitColumn)) {
            // we start from [null, min + chunk_size) and avoid [null, min)
            splits.add(ChunkRange.of(chunkStart, chunkEnd));
            // may sleep a while to avoid DDOS on PostgreSQL server
            maySleep(count++, tableId);
            chunkStart = chunkEnd;
            chunkEnd = nextChunkEnd(jdbc, chunkEnd, tableId, splitColumn, max, chunkSize);
        }
        // add the ending split
        splits.add(ChunkRange.of(chunkStart, null));
        return splits;
    }

    private SnapshotSplit createSnapshotSplit(
            JdbcConnection jdbc,
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object chunkStart,
            Object chunkEnd) {
        // currently, we only support single split column
        Object[] splitStart = chunkStart == null ? null : new Object[] {chunkStart};
        Object[] splitEnd = chunkEnd == null ? null : new Object[] {chunkEnd};
        Map<TableId, TableChanges.TableChange> schema = new HashMap<>();
        schema.put(tableId, dialect.queryTableSchema(jdbc, tableId));
        return new SnapshotSplit(
                tableId,
                splitId(tableId, chunkId),
                splitKeyType,
                splitStart,
                splitEnd,
                null,
                schema);
    }

    private String splitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }

    private void maySleep(int count, TableId tableId) {
        // every 10 queries to sleep 0.1s
        if (count % 10 == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // nothing to do
            }
            LOG.info("JdbcSourceChunkSplitter has split {} chunks for table {}", count, tableId);
        }
    }

    private Object nextChunkEnd(
            JdbcConnection jdbc,
            Object previousChunkEnd,
            TableId tableId,
            Column splitColumn,
            Object max,
            int chunkSize)
            throws SQLException {
        // chunk end might be null when max values are removed
        Object chunkEnd =
                queryNextChunkMax(jdbc, tableId, splitColumn, chunkSize, previousChunkEnd);
        if (Objects.equals(previousChunkEnd, chunkEnd)) {
            // we don't allow equal chunk start and end,
            // should query the next one larger than chunkEnd
            chunkEnd = queryMin(jdbc, tableId, splitColumn, chunkEnd);
        }
        if (isChunkEndGeMax(chunkEnd, max, splitColumn)) {
            return null;
        } else {
            return chunkEnd;
        }
    }

    protected Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, Column splitColumn)
            throws SQLException {
        return JdbcChunkUtils.queryMinMax(
                jdbc,
                jdbc.quotedTableIdString(tableId),
                jdbc.quotedColumnIdString(splitColumn.name()));
    }

    protected boolean isEvenlySplitColumn(Column splitColumn) {
        DataType flinkType = fromDbzColumn(splitColumn);
        LogicalTypeRoot typeRoot = flinkType.getLogicalType().getTypeRoot();

        // currently, we only support the optimization that split column with type BIGINT, INT,
        // DECIMAL
        return typeRoot == LogicalTypeRoot.BIGINT
                || typeRoot == LogicalTypeRoot.INTEGER
                || typeRoot == LogicalTypeRoot.DECIMAL;
    }

    /** Returns the distribution factor of the given table. */
    protected double calculateDistributionFactor(
            TableId tableId, Object min, Object max, long approximateRowCnt) {

        if (!min.getClass().equals(max.getClass())) {
            throw new IllegalStateException(
                    String.format(
                            "Unsupported operation type, the MIN value type %s is different with MAX value type %s.",
                            min.getClass().getSimpleName(), max.getClass().getSimpleName()));
        }
        if (approximateRowCnt == 0) {
            return Double.MAX_VALUE;
        }
        BigDecimal difference = ObjectUtils.minus(max, min);
        // factor = (max - min + 1) / rowCount
        final BigDecimal subRowCnt = difference.add(BigDecimal.valueOf(1));
        double distributionFactor =
                subRowCnt.divide(new BigDecimal(approximateRowCnt), 4, ROUND_CEILING).doubleValue();
        LOG.info(
                "The distribution factor of table {} is {} according to the min split key {}, max split key {} and approximate row count {}",
                tableId,
                distributionFactor,
                min,
                max,
                approximateRowCnt);
        return distributionFactor;
    }

    protected boolean isChunkEndLeMax(Object chunkEnd, Object max, Column splitColumn) {
        return ObjectUtils.compare(chunkEnd, max) <= 0;
    }

    /** ChunkEnd greater than or equal to max. */
    protected boolean isChunkEndGeMax(Object chunkEnd, Object max, Column splitColumn) {
        return ObjectUtils.compare(chunkEnd, max) >= 0;
    }
}
