package io.debezium.connector.tidb.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiDBObjectUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TiDBObjectUtils.class);

    private static TiDBEventMetadataProvider newEventMetadataProvider() {
        return new TiDBEventMetadataProvider();
    }
}
