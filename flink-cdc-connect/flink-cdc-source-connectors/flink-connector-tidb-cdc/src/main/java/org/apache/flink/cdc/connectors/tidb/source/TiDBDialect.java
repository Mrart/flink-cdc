package org.apache.flink.cdc.connectors.tidb.source;

import io.debezium.jdbc.JdbcConfiguration;
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
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBSchema;
import org.apache.flink.cdc.connectors.tidb.source.splitter.TiDBChunkSplitter;
import org.apache.flink.cdc.connectors.tidb.utils.TableDiscoveryUtils;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBConnectionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class TiDBDialect implements JdbcDataSourceDialect {

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
    JdbcConnection jdbc =
        new JdbcConnection(
            JdbcConfiguration.adapt(sourceConfig.getDbzConfiguration()),
            new JdbcConnectionFactory(sourceConfig, getPooledDataSourceFactory()),
            QUOTED_CHARACTER,
            QUOTED_CHARACTER);
    try {
      jdbc.connect();
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
    return jdbc;
  }

  @Override
  public JdbcConnectionPoolFactory getPooledDataSourceFactory() {
    return null;
  }

  @Override
  public TableChanges.TableChange queryTableSchema(JdbcConnection jdbc, TableId tableId) {
    return null;
  }

  @Override
  public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
    return null;
  }
}
