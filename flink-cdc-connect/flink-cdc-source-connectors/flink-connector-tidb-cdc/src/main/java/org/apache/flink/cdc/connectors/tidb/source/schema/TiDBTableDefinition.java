package org.apache.flink.cdc.connectors.tidb.source.schema;

import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;
import org.apache.flink.util.CollectionUtil;

import io.debezium.relational.TableId;

import java.util.List;
import java.util.stream.Collectors;

public class TiDBTableDefinition {
    TableId tableId;
    List<TiDBFieldDefinition> fieldDefinitions;
    List<String> primaryKeys;

    public TiDBTableDefinition(
            TableId tableId, List<TiDBFieldDefinition> fieldDefinitions, List<String> primaryKeys) {
        this.tableId = tableId;
        this.fieldDefinitions = fieldDefinitions;
        this.primaryKeys = primaryKeys;
    }

    public String toDdl() {
        return String.format(
                "CREATE TABLE %s (\n\t %s %s );",
                TiDBUtils.quote(tableId), fieldDefinitions(), pkDefinition());
    }

    private String fieldDefinitions() {
        return fieldDefinitions.stream()
                .map(TiDBFieldDefinition::toDdl)
                .collect(Collectors.joining(", \n\t"));
    }

    private String pkDefinition() {
        StringBuilder pkDefinition = new StringBuilder();
        if (!CollectionUtil.isNullOrEmpty(primaryKeys)) {
            pkDefinition.append(",");
            pkDefinition.append(
                    String.format(
                            "PRIMARY KEY ( %s )",
                            primaryKeys.stream()
                                    .map(TiDBUtils::quote)
                                    .collect(Collectors.joining(","))));
        }
        return pkDefinition.toString();
    }
}
