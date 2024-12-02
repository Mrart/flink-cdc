package io.debezium.connector.tidb;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TiDBPartition implements Partition {
    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public TiDBPartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final io.debezium.connector.tidb.TiDBPartition other =
                (io.debezium.connector.tidb.TiDBPartition) obj;
        return Objects.equals(serverName, other.serverName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode();
    }

    @Override
    public String toString() {
        return "TiDBPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    public static class Provider
            implements Partition.Provider<io.debezium.connector.tidb.TiDBPartition> {
        private final TiDBConnectorConfig connectorConfig;

        public Provider(TiDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<io.debezium.connector.tidb.TiDBPartition> getPartitions() {
            return Collections.singleton(
                    new io.debezium.connector.tidb.TiDBPartition(connectorConfig.getLogicalName()));
        }
    }
}
