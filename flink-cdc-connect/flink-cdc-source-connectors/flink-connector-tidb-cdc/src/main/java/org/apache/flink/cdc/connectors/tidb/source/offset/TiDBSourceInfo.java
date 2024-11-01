package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

import java.time.Instant;
import java.util.Collections;
import java.util.Set;

public class TiDBSourceInfo extends BaseSourceInfo {
  private final TiDBConnectorConfig config;

  private Instant sourceTime;
  private Set<TableId> tableIds;
  private String transactionId;

  public TiDBSourceInfo(TiDBConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  protected Instant timestamp() {
    return sourceTime;
  }

  public void setSourceTime(Instant sourceTime) {
    this.sourceTime = sourceTime;
  }

  public void tableEvent(TableId tableId) {
    this.tableIds = Collections.singleton(tableId);
  }

  @Override
  protected String database() {
    return (tableIds != null) ? tableIds.iterator().next().catalog() : null;
  }
}
