package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.relational.TableId;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TiDBSourceInfo extends BaseSourceInfo {
    private final TiDBConnectorConfig config;
    public static final String COMMIT_VERSION_KEY = "commitVersion";
    private Long commitVersion = -1L;
    private Instant sourceTime;
    private Set<TableId> tableIds;
    private String databaseName;

    public TiDBSourceInfo(TiDBConnectorConfig config) {
        super(config);
        this.config = config;
        this.tableIds = new HashSet<>();
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    public void setSourceTime(Instant sourceTime) {
        this.sourceTime = sourceTime;
    }

    public Long getCommitVersion() {
        return commitVersion;
    }

    public void setCommitVersion(long commitVersion) {
        this.commitVersion = commitVersion;
    }

    public void databaseEvent(String databaseName) {
        this.databaseName = databaseName;
    }

    public void tableEvent(Set<TableId> tableIds) {
        this.tableIds = new HashSet<>(tableIds);
    }

    public void tableEvent(TableId tableId) {
        this.tableIds = Collections.singleton(tableId);
    }

    @Override
    protected String database() {
        //        return (tableIds != null) ? tableIds.iterator().next().catalog() : null;
        if (tableIds == null || tableIds.isEmpty()) {
            return databaseName;
        }
        final TableId tableId = tableIds.iterator().next();
        if (tableId == null) {
            return databaseName;
        }
        return tableId.catalog();
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
}
