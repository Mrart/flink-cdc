package io.debezium.connector.tidb.connection;

import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBObjectUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TiDBObjectUtils.class);

    private static  TiDBEventMetadataProvider newEventMetadataProvider(){
        return new TiDBEventMetadataProvider();
    }
}
