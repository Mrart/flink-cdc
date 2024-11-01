package org.apache.flink.cdc.connectors.tidb.utils;

import io.debezium.relational.Column;
import io.debezium.relational.Key;
import io.debezium.relational.Table;

import java.util.List;

public class CustomeKeyMapper implements Key.KeyMapper{
    @Override
    public List<Column> getKeyKolumns(Table table) {
        return null;
    }
}
