package org.apache.flink.cdc.connectors.tidb.utils;

import io.debezium.connector.mysql.*;
import io.debezium.connector.tidb.TidbTopicSelector;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.converter.TiDBValueConverters;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;
import org.apache.flink.util.FlinkRuntimeException;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TiDBConnectionUtils {

  public static boolean isTableIdCaseSensitive(JdbcConnection connection) {
    return !"0"
        .equals(
            readMySqlSystemVariables(connection).get(MySqlSystemVariables.LOWER_CASE_TABLE_NAMES));
  }

  public static Map<String, String> readMySqlSystemVariables(JdbcConnection connection) {
    // Read the system variables from the MySQL instance and get the current database name ...
    return querySystemVariables(connection, "SHOW VARIABLES");
  }

  private static Map<String, String> querySystemVariables(
      JdbcConnection connection, String statement) {
    final Map<String, String> variables = new HashMap<>();
    try {
      connection.query(
          statement,
          rs -> {
            while (rs.next()) {
              String varName = rs.getString(1);
              String value = rs.getString(2);
              if (varName != null && value != null) {
                variables.put(varName, value);
              }
            }
          });
    } catch (SQLException e) {
      throw new FlinkRuntimeException("Error reading TiDB variables: " + e.getMessage(), e);
    }

    return variables;
  }


  //MysqlValueConverters
  private static TiDBValueConverters getValueConverters(TiDBConnectorConfig dbzTiDBConfig) {
    TemporalPrecisionMode timePrecisionMode = dbzTiDBConfig.getTemporalPrecisionMode();
    JdbcValueConverters.DecimalMode decimalMode = dbzTiDBConfig.getDecimalMode();
    String bigIntUnsignedHandlingModeStr =
            dbzTiDBConfig
                    .getConfig()
                    .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
    MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
            MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                    bigIntUnsignedHandlingModeStr);
    JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
            bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

    // dbzTiDBConfig.getConfig().getBoolean(TiDBConnectorConfig.ENABLE_TIME_ADJUSTER)
    boolean timeAdjusterEnabled = false;
    return new TiDBValueConverters(
            decimalMode,
            timePrecisionMode,
            bigIntUnsignedMode,
            dbzTiDBConfig.binaryHandlingMode(),
            timeAdjusterEnabled ? TiDBValueConverters::adjustTemporal : x -> x,
           TiDBValueConverters::defaultParsingErrorHandler);
  }

  public static TiDBDatabaseSchema createTiDBDatabaseSchema(
          TiDBConnectorConfig dbzTiDBConfig, boolean isTableIdCaseSensitive) {
    TopicSelector<TableId> topicSelector = TidbTopicSelector.defaultSelector(dbzTiDBConfig);
    return new TiDBDatabaseSchema(
            dbzTiDBConfig,
            topicSelector,
            isTableIdCaseSensitive);
  }
}
