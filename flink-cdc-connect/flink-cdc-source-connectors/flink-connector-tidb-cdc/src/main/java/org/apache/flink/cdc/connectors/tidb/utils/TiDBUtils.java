package org.apache.flink.cdc.connectors.tidb.utils;

import io.debezium.connector.tidb.TidbTopicSelector;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.Key;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Collectors;

public class TiDBUtils {
  private static final String BIT = "BIT";
  private static final String TINYINT = "TINYINT";
  private static final String TINYINT_UNSIGNED = "TINYINT UNSIGNED";
  private static final String SMALLINT = "SMALLINT";
  private static final String SMALLINT_UNSIGNED = "SMALLINT UNSIGNED";
  private static final String MEDIUMINT = "MEDIUMINT";
  private static final String MEDIUMINT_UNSIGNED = "MEDIUMINT UNSIGNED";
  private static final String INT = "INT";
  private static final String INT_UNSIGNED = "INT UNSIGNED";
  private static final String BIGINT = "BIGINT";
  private static final String BIGINT_UNSIGNED = "BIGINT UNSIGNED";
  private static final String FLOAT = "FLOAT";
  private static final String FLOAT_UNSIGNED = "FLOAT UNSIGNED";
  private static final String DOUBLE = "DOUBLE";
  private static final String DOUBLE_UNSIGNED = "DOUBLE UNSIGNED";
  private static final String DECIMAL = "DECIMAL";
  private static final String DECIMAL_UNSIGNED = "DECIMAL UNSIGNED";
  private static final String CHAR = "CHAR";
  private static final String VARCHAR = "VARCHAR";
  private static final String TINYTEXT = "TINYTEXT";
  private static final String MEDIUMTEXT = "MEDIUMTEXT";
  private static final String TEXT = "TEXT";
  private static final String LONGTEXT = "LONGTEXT";
  private static final String DATE = "DATE";
  private static final String TIME = "TIME";
  private static final String DATETIME = "DATETIME";
  private static final String TIMESTAMP = "TIMESTAMP";
  private static final String YEAR = "YEAR";
  private static final String BINARY = "BINARY";
  private static final String VARBINARY = "VARBINARY";
  private static final String TINYBLOB = "TINYBLOB";
  private static final String MEDIUMBLOB = "MEDIUMBLOB";
  private static final String BLOB = "BLOB";
  private static final String LONGBLOB = "LONGBLOB";
  private static final String JSON = "JSON";
  private static final String SET = "SET";
  private static final String ENUM = "ENUM";
  private static final String GEOMETRY = "GEOMETRY";
  private static final String UNKNOWN = "UNKNOWN";

  public static Object queryNextChunkMax(
      JdbcConnection jdbc,
      TableId tableId,
      String splitColumnName,
      int chunkSize,
      Object includedLowerBound)
      throws SQLException {
    String quotedColumn = jdbc.quotedColumnIdString(splitColumnName);
    String query =
        String.format(
            "SELECT MAX(%s) FROM ("
                + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                + ") AS T",
            quotedColumn,
            quotedColumn,
            jdbc.quotedTableIdString(tableId),
            quotedColumn,
            quotedColumn,
            chunkSize);
    return jdbc.prepareQueryAndMap(
        query,
        ps -> ps.setObject(1, includedLowerBound),
        rs -> {
          if (!rs.next()) {
            // this should never happen
            throw new SQLException(
                String.format("No result returned after running query [%s]", query));
          }
          return rs.getObject(1);
        });
  }

  public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
      throws SQLException {
    // The statement used to get approximate row count which is less
    // accurate than COUNT(*), but is more efficient for large table.
    final String useDatabaseStatement = String.format("USE %s;", quote(tableId.catalog()));
    final String rowCountQuery = String.format("SHOW TABLE STATUS LIKE '%s';", tableId.table());
    jdbc.execute(useDatabaseStatement);
    return jdbc.queryAndMap(
        rowCountQuery,
        rs -> {
          if (!rs.next() || rs.getMetaData().getColumnCount() < 5) {
            throw new SQLException(
                String.format("No result returned after running query [%s]", rowCountQuery));
          }
          return rs.getLong(5);
        });
  }

  public static DataType fromDbzColumn(Column column) {
    DataType dataType = convertFromColumn(column);
    if (column.isOptional()) {
      return dataType;
    } else {
      return dataType.notNull();
    }
  }

  private static DataType convertFromColumn(Column column) {
    String typeName = column.typeName();
    switch (typeName) {
      case TINYINT:
        return column.length() == 1 ? DataTypes.BOOLEAN() : DataTypes.TINYINT();
      case TINYINT_UNSIGNED:
      case SMALLINT:
        return DataTypes.SMALLINT();
      case SMALLINT_UNSIGNED:
      case INT:
      case MEDIUMINT:
        return DataTypes.INT();
      case INT_UNSIGNED:
      case MEDIUMINT_UNSIGNED:
      case BIGINT:
        return DataTypes.BIGINT();
      case BIGINT_UNSIGNED:
        return DataTypes.DECIMAL(20, 0);
      case FLOAT:
        return DataTypes.FLOAT();
      case DOUBLE:
        return DataTypes.DOUBLE();
      case DECIMAL:
        return DataTypes.DECIMAL(column.length(), column.scale().orElse(0));
      case TIME:
        return column.length() >= 0 ? DataTypes.TIME(column.length()) : DataTypes.TIME();
      case DATE:
        return DataTypes.DATE();
      case DATETIME:
      case TIMESTAMP:
        return column.length() >= 0 ? DataTypes.TIMESTAMP(column.length()) : DataTypes.TIMESTAMP();
      case CHAR:
        return DataTypes.CHAR(column.length());
      case VARCHAR:
        return DataTypes.VARCHAR(column.length());
      case TEXT:
        return DataTypes.STRING();
      case BINARY:
        return DataTypes.BINARY(column.length());
      case VARBINARY:
        return DataTypes.VARBINARY(column.length());
      case BLOB:
        return DataTypes.BYTES();
      default:
        throw new UnsupportedOperationException(
            String.format("Don't support MySQL type '%s' yet.", typeName));
    }
  }

  public static String quote(String dbOrTableName) {
    return "`" + dbOrTableName + "`";
  }

  public static String quote(TableId tableId) {
    return tableId.toQuotedString('`');
  }

  public static TiDBDatabaseSchema createTiDBDatabaseSchema(
          TiDBConnectorConfig dbzTiDBConfig, boolean isTableIdCaseSensitive) {
    TopicSelector<TableId> topicSelector = TidbTopicSelector.defaultSelector(dbzTiDBConfig);
    Key.KeyMapper customKeysMapper = new CustomeKeyMapper();
    return new TiDBDatabaseSchema(
            dbzTiDBConfig,
            topicSelector,
            isTableIdCaseSensitive,
            customKeysMapper);
  }


  public static PreparedStatement readTableSplitDataStatement(
          JdbcConnection jdbc,
          String sql,
          boolean isFirstSplit,
          boolean isLastSplit,
          Object[] splitStart,
          Object[] splitEnd,
          int primaryKeyNum,
          int fetchSize) {
    try {
      final PreparedStatement statement = initStatement(jdbc, sql, fetchSize);
      if (isFirstSplit && isLastSplit) {
        return statement;
      }
      if (isFirstSplit) {
        for (int i = 0; i < primaryKeyNum; i++) {
          statement.setObject(i + 1, splitEnd[i]);
          statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
        }
      } else if (isLastSplit) {
        for (int i = 0; i < primaryKeyNum; i++) {
          statement.setObject(i + 1, splitStart[i]);
        }
      } else {
        for (int i = 0; i < primaryKeyNum; i++) {
          statement.setObject(i + 1, splitStart[i]);
          statement.setObject(i + 1 + primaryKeyNum, splitEnd[i]);
          statement.setObject(i + 1 + 2 * primaryKeyNum, splitEnd[i]);
        }
      }
      return statement;
    } catch (Exception e) {
      throw new RuntimeException("Failed to build the split data read statement.", e);
    }
  }

  private static PreparedStatement initStatement(JdbcConnection jdbc, String sql, int fetchSize)
          throws SQLException {
    final Connection connection = jdbc.connection();
    connection.setAutoCommit(false);
    final PreparedStatement statement = connection.prepareStatement(sql);
    statement.setFetchSize(fetchSize);
    return statement;
  }

  public static String buildSplitScanQuery(
          TableId tableId, RowType pkRowType, boolean isFirstSplit, boolean isLastSplit) {
    return buildSplitQuery(tableId, pkRowType, isFirstSplit, isLastSplit, -1, true);
  }
  private static String buildSplitQuery(
          TableId tableId,
          RowType pkRowType,
          boolean isFirstSplit,
          boolean isLastSplit,
          int limitSize,
          boolean isScanningData) {
    final String condition;

    if (isFirstSplit && isLastSplit) {
      condition = null;
    } else if (isFirstSplit) {
      final StringBuilder sql = new StringBuilder();
      addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
      if (isScanningData) {
        sql.append(" AND NOT (");
        addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
        sql.append(")");
      }
      condition = sql.toString();
    } else if (isLastSplit) {
      final StringBuilder sql = new StringBuilder();
      addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
      condition = sql.toString();
    } else {
      final StringBuilder sql = new StringBuilder();
      addPrimaryKeyColumnsToCondition(pkRowType, sql, " >= ?");
      if (isScanningData) {
        sql.append(" AND NOT (");
        addPrimaryKeyColumnsToCondition(pkRowType, sql, " = ?");
        sql.append(")");
      }
      sql.append(" AND ");
      addPrimaryKeyColumnsToCondition(pkRowType, sql, " <= ?");
      condition = sql.toString();
    }

    if (isScanningData) {
      return buildSelectWithRowLimits(
              tableId, limitSize, "*", Optional.ofNullable(condition), Optional.empty());
    } else {
      final String orderBy =
              pkRowType.getFieldNames().stream().collect(Collectors.joining(", "));
      return buildSelectWithBoundaryRowLimits(
              tableId,
              limitSize,
              getPrimaryKeyColumnsProjection(pkRowType),
              getMaxPrimaryKeyColumnsProjection(pkRowType),
              Optional.ofNullable(condition),
              orderBy);
    }
  }

  private static void addPrimaryKeyColumnsToCondition(
          RowType pkRowType, StringBuilder sql, String predicate) {
    for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
         fieldNamesIt.hasNext(); ) {
      sql.append(fieldNamesIt.next()).append(predicate);
      if (fieldNamesIt.hasNext()) {
        sql.append(" AND ");
      }
    }
  }

  private static String buildSelectWithBoundaryRowLimits(
          TableId tableId,
          int limit,
          String projection,
          String maxColumnProjection,
          Optional<String> condition,
          String orderBy) {
    final StringBuilder sql = new StringBuilder("SELECT ");
    sql.append(maxColumnProjection);
    sql.append(" FROM (");
    sql.append("SELECT ");
    sql.append(projection);
    sql.append(" FROM ");
    sql.append(quotedTableIdString(tableId));
    if (condition.isPresent()) {
      sql.append(" WHERE ").append(condition.get());
    }
    sql.append(" ORDER BY ").append(orderBy).append(" LIMIT ").append(limit);
    sql.append(") T");
    return sql.toString();
  }

  private static String quotedTableIdString(TableId tableId) {
    return tableId.toQuotedString('`');
  }

  private static String buildSelectWithRowLimits(
          TableId tableId,
          int limit,
          String projection,
          Optional<String> condition,
          Optional<String> orderBy) {
    final StringBuilder sql = new StringBuilder("SELECT ");
    sql.append(projection).append(" FROM ");
    sql.append(quotedTableIdString(tableId));
    if (condition.isPresent()) {
      sql.append(" WHERE ").append(condition.get());
    }
    if (orderBy.isPresent()) {
      sql.append(" ORDER BY ").append(orderBy.get());
    }
    if (limit > 0) {
      sql.append(" LIMIT ").append(limit);
    }
    return sql.toString();
  }

  private static String getPrimaryKeyColumnsProjection(RowType pkRowType) {
    StringBuilder sql = new StringBuilder();
    for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
         fieldNamesIt.hasNext(); ) {
      sql.append(fieldNamesIt.next());
      if (fieldNamesIt.hasNext()) {
        sql.append(" , ");
      }
    }
    return sql.toString();
  }

  private static String getMaxPrimaryKeyColumnsProjection(RowType pkRowType) {
    StringBuilder sql = new StringBuilder();
    for (Iterator<String> fieldNamesIt = pkRowType.getFieldNames().iterator();
         fieldNamesIt.hasNext(); ) {
      sql.append("MAX(" + fieldNamesIt.next() + ")");
      if (fieldNamesIt.hasNext()) {
        sql.append(" , ");
      }
    }
    return sql.toString();
  }

  public static BinlogOffset currentBinlogOffset(JdbcConnection jdbc) {
    final String showMasterStmt = "SHOW MASTER STATUS";
    try {
      return jdbc.queryAndMap(
              showMasterStmt,
              rs -> {
                if (rs.next()) {
                  final String binlogFilename = rs.getString(1);
                  final long binlogPosition = rs.getLong(2);
                  final String gtidSet =
                          rs.getMetaData().getColumnCount() > 4 ? rs.getString(5) : null;
                  return new BinlogOffset(
                          binlogFilename, binlogPosition, 0L, 0, 0, gtidSet, null);
                } else {
                  throw new FlinkRuntimeException(
                          "Cannot read the binlog filename and position via '"
                                  + showMasterStmt
                                  + "'. Make sure your server is correctly configured");
                }
              });
    } catch (SQLException e) {
      throw new FlinkRuntimeException(
              "Cannot read the binlog filename and position via '"
                      + showMasterStmt
                      + "'. Make sure your server is correctly configured",
              e);
    }
  }
}
