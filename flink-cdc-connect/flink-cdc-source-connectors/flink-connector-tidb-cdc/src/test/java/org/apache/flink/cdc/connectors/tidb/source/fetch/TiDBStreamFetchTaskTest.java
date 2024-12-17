package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.split.ChangeEventRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceRecords;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReaderContext;
import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceSplitReader;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetFactory;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;

public class TiDBStreamFetchTaskTest extends TiDBTestBase {
  private static final String databaseName = "customer";
  private static final String tableName = "customers";
  private static final String STREAM_SPLIT_ID = "stream-split";

  private static final int USE_POST_LOWWATERMARK_HOOK = 1;
  private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;
  private static final int MAX_RETRY_TIMES = 100;

  private TiDBSourceConfig sourceConfig;
  private TiDBDialect tiDBDialect;
  private CDCEventOffsetFactory cdcEventOffsetFactory;

  @Before
  public void before() {
    initializeTidbTable("customer");
    TiDBSourceConfigFactory tiDBSourceConfigFactory = new TiDBSourceConfigFactory();
    tiDBSourceConfigFactory.pdAddresses(
        PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN));
    tiDBSourceConfigFactory.hostname(TIDB.getHost());
    tiDBSourceConfigFactory.port(TIDB.getMappedPort(TIDB_PORT));
    tiDBSourceConfigFactory.username(TiDBTestBase.TIDB_USER);
    tiDBSourceConfigFactory.password(TiDBTestBase.TIDB_PASSWORD);
    tiDBSourceConfigFactory.databaseList(this.databaseName);
    tiDBSourceConfigFactory.tableList(this.databaseName + "." + this.tableName);
    tiDBSourceConfigFactory.splitSize(10);
    tiDBSourceConfigFactory.skipSnapshotBackfill(true);
    tiDBSourceConfigFactory.scanNewlyAddedTableEnabled(true);
    this.sourceConfig = tiDBSourceConfigFactory.create(0);
    this.tiDBDialect = new TiDBDialect(tiDBSourceConfigFactory.create(0));
    this.cdcEventOffsetFactory = new CDCEventOffsetFactory();
  }

  @Test
  public void testStreamSplitReader() throws Exception {
    String tableId = databaseName + "." + tableName;
    IncrementalSourceReaderContext incrementalSourceReaderContext =
        new IncrementalSourceReaderContext(new TestingReaderContext());
    IncrementalSourceSplitReader<JdbcSourceConfig> streamSplitReader =
        new IncrementalSourceSplitReader<>(
            0,
            tiDBDialect,
            sourceConfig,
            incrementalSourceReaderContext,
            SnapshotPhaseHooks.empty());
    try {
      CDCEventOffset startOffset = new CDCEventOffset(Instant.now().getEpochSecond());
      String[] insertDataSql =
          new String[] {
            "INSERT INTO " + tableId + " VALUES(112, 'user_12','Shanghai','123567891234')",
            "INSERT INTO " + tableId + " VALUES(113, 'user_13','Shanghai','123567891234')",
          };
      try (TiDBConnection tiDBConnection = tiDBDialect.openJdbcConnection()) {
        tiDBConnection.execute(insertDataSql);
        tiDBConnection.commit();
      }
      StreamSplit streamSplit =
          new StreamSplit(
              STREAM_SPLIT_ID,
              startOffset,
              cdcEventOffsetFactory.createNoStoppingOffset(),
              new ArrayList<>(),
              new HashMap<>(),
              0);
      assertTrue(streamSplitReader.canAssignNextSplit());
      streamSplitReader.handleSplitsChanges(new SplitsAddition<>(singletonList(streamSplit)));
      int retry = 0;
      int count = 0;
      while (retry < MAX_RETRY_TIMES) {
        ChangeEventRecords records = (ChangeEventRecords) streamSplitReader.fetch();
        if (records.nextSplit() != null) {
          SourceRecords sourceRecords;
          while ((sourceRecords = records.nextRecordFromSplit()) != null) {
            Iterator<SourceRecord> iterator = sourceRecords.iterator();
            while (iterator.hasNext()) {
              Struct value = (Struct) iterator.next().value();
              //              OperationType operationType =
              //                  OperationType.fromString(value.getString(OPERATION_TYPE_FIELD));

              //              assertEquals(OperationType.INSERT, operationType);
              //              BsonDocument fullDocument =
              // BsonDocument.parse(value.getString(FULL_DOCUMENT_FIELD));
              //              long productNo = fullDocument.getInt64("product_no").longValue();
              //              String productKind =
              // fullDocument.getString("product_kind").getValue();
              //              String userId = fullDocument.getString("user_id").getValue();
              //              String description = fullDocument.getString("description").getValue();
              //
              //              assertEquals("KIND_" + productNo, productKind);
              //              assertEquals("user_" + productNo, userId);
              //              assertEquals("my shopping cart " + productNo, description);

              if (++count >= insertDataSql.length) {
                return;
              }
            }
          }
        } else {
          break;
        }
      }
    } finally {
      streamSplitReader.close();
    }
  }

  @Test
  public void testChangingDataInStream() throws Exception {
    initializeTidbTable("customer");
    String tableId = databaseName + "." + tableName;
    String[] changingDataSql =
        new String[] {
          "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
          "DELETE FROM " + tableId + " where id = 102",
          "INSERT INTO " + tableId + " VALUES(102, 'user_2','hangzhou','123567891234')",
          "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
          "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
          "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
        };

    List<String> actual = getDataInStreamRead(changingDataSql, USE_PRE_HIGHWATERMARK_HOOK);
  }

  private List<String> getDataInStreamRead(String[] changingDataSql, int hookType)
      throws SQLException {
    SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
    try (TiDBConnection tiDBConnection = tiDBDialect.openJdbcConnection()) {
      SnapshotPhaseHook snapshotPhaseHook =
          (tidbSourceConfig, split) -> {
            tiDBConnection.execute(changingDataSql);
            tiDBConnection.commit();
            try {
              Thread.sleep(500L);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          };
      if (hookType == USE_POST_LOWWATERMARK_HOOK) {
        hooks.setPostLowWatermarkAction(snapshotPhaseHook);
      } else if (hookType == USE_PRE_HIGHWATERMARK_HOOK) {
        hooks.setPreHighWatermarkAction(snapshotPhaseHook);
      }
    }
    return null;
  }
}
