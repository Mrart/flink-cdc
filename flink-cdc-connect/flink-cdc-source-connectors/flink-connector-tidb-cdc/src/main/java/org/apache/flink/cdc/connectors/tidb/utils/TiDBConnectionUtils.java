package org.apache.flink.cdc.connectors.tidb.utils;

import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.jdbc.JdbcConnection;
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
}
