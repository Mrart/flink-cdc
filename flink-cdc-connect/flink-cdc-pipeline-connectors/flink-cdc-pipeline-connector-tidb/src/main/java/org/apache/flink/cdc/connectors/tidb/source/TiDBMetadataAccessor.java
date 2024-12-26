package org.apache.flink.cdc.connectors.tidb.source;

import io.debezium.connector.tidb.TiDBPartition;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBSchemaUtils;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/** {@link MetadataAccessor} for {@link TiDBMetadataAccessor}. */
@Internal
public class TiDBMetadataAccessor implements MetadataAccessor {

    private final TiDBSourceConfig tiDBSourceConfig;

    private final TiDBPartition tiDBPartition;

    public TiDBMetadataAccessor(
            TiDBSourceConfig tiDBSourceConfig) {
        this.tiDBSourceConfig = tiDBSourceConfig;
        this.tiDBPartition = new TiDBPartition(tiDBSourceConfig.getDbzConnectorConfig().getLogicalName());
    }


    @Override
    public List<String> listNamespaces() {
        throw new UnsupportedOperationException("List namespace is not supported by TiDB.");
    }

    @Override
    public List<String> listSchemas(@Nullable String namespace) {
        return TiDBSchemaUtils.listDatabase(tiDBSourceConfig);
    }

    @Override
    public List<TableId> listTables(@Nullable String namespace, @Nullable String dbName) {
        return TiDBSchemaUtils.listTables(tiDBSourceConfig,dbName);
    }

    @Override
    public Schema getTableSchema(TableId tableId) throws SQLException {
        return TiDBSchemaUtils.getTableSchema(tiDBSourceConfig,tiDBPartition,tableId);
    }
}
