package org.apache.flink.cdc.connectors.tidb.source;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionFactory;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.reader.external.FetchTask;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnectionPoolFactory;
import org.apache.flink.cdc.connectors.tidb.source.fetch.TiDBScanFetchTask;
import org.apache.flink.cdc.connectors.tidb.source.fetch.TiDBStreamFetchTask;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBSchema;
import org.apache.flink.cdc.connectors.tidb.source.splitter.TiDBChunkSplitter;
import org.apache.flink.cdc.connectors.tidb.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBConnectionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBDialect implements JdbcDataSourceDialect {
  private static final Logger LOG = LoggerFactory.getLogger(TiDBDialect.class);

  private static final String QUOTED_CHARACTER = "`";
  private static final long serialVersionUID = 1L;

  private final TiDBSourceConfig sourceConfig;
  private transient TiDBSchema tiDBSchema;

  public TiDBDialect(TiDBSourceConfig sourceConfig) {
    this.sourceConfig = sourceConfig;

  }

  @Override
  public String getName() {
    return "TiDB";
  }

  @Override
  public Offset displayCurrentOffset(JdbcSourceConfig sourceConfig) {
    return null;
  }

  @Override
  public boolean isDataCollectionIdCaseSensitive(JdbcSourceConfig sourceConfig) {
    try (JdbcConnection jdbcConnection = openJdbcConnection(sourceConfig)) {
      return TiDBConnectionUtils.isTableIdCaseSensitive(jdbcConnection);
    } catch (SQLException e) {
      throw new FlinkRuntimeException("Error reading TiDB variables: " + e.getMessage(), e);
    }
  }

  @Override
  public ChunkSplitter createChunkSplitter(JdbcSourceConfig sourceConfig) {
    return new TiDBChunkSplitter(sourceConfig,this);
  }

  @Override
  public FetchTask.Context createFetchTaskContext(JdbcSourceConfig sourceConfig) {
    return null;
  }

  @Override
  public boolean isIncludeDataCollection(JdbcSourceConfig sourceConfig, TableId tableId) {
    return false;
  }

  @Override
  public List<TableId> discoverDataCollections(JdbcSourceConfig sourceConfig) {
    try (JdbcConnection jdbc = openJdbcConnection(sourceConfig)){
      return TableDiscoveryUtils.listTables(
              sourceConfig.getDatabaseList().get(0),jdbc,sourceConfig.getTableFilters());
    }catch (SQLException e){
      throw  new FlinkRuntimeException("Error to discover tables:" + e.getMessage(),e);
    }
  }

  @Override
  public Map<TableId, TableChanges.TableChange> discoverDataCollectionSchemas(
          JdbcSourceConfig sourceConfig) {
    return null;
  }

  @Override
  public JdbcConnection openJdbcConnection(JdbcSourceConfig sourceConfig) {
    TiDBSourceConfig tiDBSourceConfig = (TiDBSourceConfig) sourceConfig;
    TiDBConnectorConfig dbzConfig = tiDBSourceConfig.getDbzConnectorConfig();

    JdbcConnection jdbc =
            new TiDBConnection(
                    dbzConfig.getJdbcConfig(),
                    new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()),
                    QUOTED_CHARACTER,
                    QUOTED_CHARACTER);
    try {
      jdbc.connect();
    } catch (Exception e) {
      LOG.error("Failed to open TiDB connection", e);
      throw new FlinkRuntimeException(e);
    }
    return jdbc;
  }

  public TiDBConnection openJdbcConnection() {
    return (TiDBConnection) openJdbcConnection(sourceConfig);
  }

  @Override
  public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
    return new TiDBConnectionPoolFactory();
  }

  @Override
  public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
    if(tiDBSchema == null) {
      tiDBSchema = new TiDBSchema(sourceConfig,isDataCollectionIdCaseSensitive(sourceConfig));
    }
    return tiDBSchema.getTableSchema(jdbc,tableId);
  }

  @Override
  public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
    if (sourceSplitBase.isSnapshotSplit()) {
      return new TiDBScanFetchTask(sourceSplitBase.asSnapshotSplit());
    } else {
      return new TiDBStreamFetchTask(sourceSplitBase.asStreamSplit());
    }
  }
}