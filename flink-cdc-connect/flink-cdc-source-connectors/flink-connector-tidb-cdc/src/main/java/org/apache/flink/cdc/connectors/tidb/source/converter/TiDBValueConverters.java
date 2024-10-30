package org.apache.flink.cdc.connectors.tidb.source.converter;

import io.debezium.jdbc.JdbcValueConverters;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

/** JdbcValueConverters for tiDB. */
public class TiDBValueConverters extends JdbcValueConverters {
  public TiDBValueConverters(TiDBConnectorConfig config) {}
}
