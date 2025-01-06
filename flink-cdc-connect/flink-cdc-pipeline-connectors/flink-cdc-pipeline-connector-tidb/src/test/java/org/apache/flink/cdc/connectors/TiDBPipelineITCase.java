package org.apache.flink.cdc.connectors;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.factory.TiDBDataSourceFactory;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDataSource;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.CloseableIterator;

import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class TiDBPipelineITCase extends TiDBTestBase {
    private static final String databaseName = "inventory";
    private static final String tableName = "products";

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    @Ignore
    public void testSnapshotStartMode() throws Exception {
        initializeTidbTable("inventory_pipeline");
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("weight", DataTypes.FLOAT()));
        TiDBSourceConfigFactory jdbcSourceConfigFactory =
                (TiDBSourceConfigFactory)
                        new TiDBSourceConfigFactory()
                                .hostname(TIDB.getHost())
                                .port(TIDB.getMappedPort(TIDB_PORT))
                                .username(TiDBTestBase.TIDB_USER)
                                .password(TiDBTestBase.TIDB_PASSWORD)
                                .databaseList(databaseName)
                                .chunkKeyColumn("name")
                                .tableList(this.databaseName + "." + this.tableName)
                                .startupOptions(StartupOptions.snapshot())
                                .includeSchemaChanges(false)
                                .chunkKeyColumn("inventory.table1:column1;inventory.table2:column2")
                                .splitSize(10);
        FlinkSourceProvider eventSourceProvider =
                (FlinkSourceProvider)
                        new TiDBDataSource(jdbcSourceConfigFactory).getEventSourceProvider();

        CloseableIterator<Event> eventDataStreamSource =
                env.fromSource(
                                eventSourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                TiDBDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        //        waitForSinkSize("sink", 9);

        TableId tableId = TableId.tableId(databaseName, tableName);
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        List<Event> expectedSnapshot = getSnapshotExpected(tableId);

        //        try (Connection connection = getJdbcConnection("inventory");
        //             Statement statement = connection.createStatement()){
        //        }

        List<Event> actual =
                fetchResultsExcept(
                        eventDataStreamSource, expectedSnapshot.size(), createTableEvent);
        assertThat(actual.subList(0, expectedSnapshot.size()))
                .containsExactlyInAnyOrder(expectedSnapshot.toArray(new Event[0]));
    }

    private static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    private static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    private static <T> List<T> fetchResultsExcept(Iterator<T> iter, int size, T sideEvent) {
        List<T> result = new ArrayList<>(size);
        List<T> sideResults = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (!event.equals(sideEvent)) {
                result.add(event);
                size--;
            } else {
                sideResults.add(sideEvent);
            }
        }
        // Also ensure we've received at least one or many side events.
        assertThat(sideResults).isNotEmpty();
        return result;
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn(
                                "id", org.apache.flink.cdc.common.types.DataTypes.INT().notNull())
                        .physicalColumn(
                                "name",
                                org.apache.flink.cdc.common.types.DataTypes.VARCHAR(255).notNull(),
                                null,
                                "flink")
                        .physicalColumn(
                                "description",
                                org.apache.flink.cdc.common.types.DataTypes.VARCHAR(512))
                        .physicalColumn(
                                "weight",
                                org.apache.flink.cdc.common.types.DataTypes.DECIMAL(10, 3))
                        .primaryKey(Collections.singletonList("id"))
                        .build());
    }

    private List<Event> getSnapshotExpected(TableId tableId) {
        RowType rowType =
                RowType.of(
                        new org.apache.flink.cdc.common.types.DataType[] {
                            org.apache.flink.cdc.common.types.DataTypes.INT().notNull(),
                            org.apache.flink.cdc.common.types.DataTypes.VARCHAR(255).notNull(),
                            org.apache.flink.cdc.common.types.DataTypes.VARCHAR(512),
                            org.apache.flink.cdc.common.types.DataTypes.DECIMAL(10, 3)
                        },
                        new String[] {"id", "name", "description", "weight"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        List<Event> snapshotExpected = new ArrayList<>();
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101,
                                    BinaryStringData.fromString("scooter"),
                                    BinaryStringData.fromString("Small 2-wheel scooter"),
                                    DecimalData.fromBigDecimal(new BigDecimal("3.14"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102,
                                    BinaryStringData.fromString("car battery"),
                                    BinaryStringData.fromString("12V car battery"),
                                    DecimalData.fromBigDecimal(new BigDecimal("8.1"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    103,
                                    BinaryStringData.fromString("12-pack drill bits"),
                                    BinaryStringData.fromString(
                                            "12-pack of drill bits with sizes ranging from #40 to #3"),
                                    DecimalData.fromBigDecimal(new BigDecimal("0.8"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("12oz carpenter's hammer"),
                                    DecimalData.fromBigDecimal(new BigDecimal("0.75"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("14oz carpenter's hammer"),
                                    DecimalData.fromBigDecimal(new BigDecimal("0.875"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    106,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("16oz carpenter's hammer"),
                                    DecimalData.fromBigDecimal(new BigDecimal("1"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    107,
                                    BinaryStringData.fromString("rocks"),
                                    BinaryStringData.fromString("box of assorted rocks"),
                                    DecimalData.fromBigDecimal(new BigDecimal("5.3"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    108,
                                    BinaryStringData.fromString("jacket"),
                                    BinaryStringData.fromString(
                                            "water resistent black wind breaker"),
                                    DecimalData.fromBigDecimal(new BigDecimal("0.1"), 10, 3)
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    109,
                                    BinaryStringData.fromString("spare tire"),
                                    BinaryStringData.fromString("24 inch spare tire"),
                                    DecimalData.fromBigDecimal(new BigDecimal("22.2"), 10, 3)
                                })));
        return snapshotExpected;
    }
}
