package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TiDBSourceInfo extends BaseSourceInfo {
  private final TiDBConnectorConfig config;
  public static final String TRANSACTION_ID_KEY = "transaction_id";
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

  public String tableSchema() {
    return (tableIds == null || tableIds.isEmpty())
        ? null
        : tableIds.stream()
            .filter(Objects::nonNull)
            .map(TableId::schema)
            .filter(Objects::nonNull)
            .distinct()
            .collect(Collectors.joining(","));
  }

  public String table() {
    return (tableIds == null || tableIds.isEmpty())
        ? null
        : tableIds.stream()
            .filter(Objects::nonNull)
            .map(TableId::table)
            .collect(Collectors.joining(","));
  }

  public String transactionId() {
    return transactionId;
  }
}
