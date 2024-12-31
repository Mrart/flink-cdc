package org.apache.flink.cdc.connectors.tidb.utils;

import com.esotericsoftware.minlog.Log;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TableDiscoveryUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    public static List<TableId> listTables(
            String database, JdbcConnection jdbc, RelationalTableFilters tableFilters)
            throws SQLException {

        Set<TableId> allTableIds =
                jdbc.readTableNames(database, null, null, new String[] {"TABLE"});

        Set<TableId> capturedTables =
                allTableIds.stream()
                        .filter(t -> tableFilters.dataCollectionFilter().isIncluded(t))
                        .collect(Collectors.toSet());
        LOG.info("listTables include paramaters：database:{}",database);
        LOG.info(
                "TiDB captured tables : {} .",
                capturedTables.stream().map(TableId::toString).collect(Collectors.joining(",")));

        return new ArrayList<>(capturedTables);
    }
}
