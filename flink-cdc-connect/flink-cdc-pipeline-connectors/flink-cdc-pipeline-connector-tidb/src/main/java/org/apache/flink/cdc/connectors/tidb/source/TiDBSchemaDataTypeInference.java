package org.apache.flink.cdc.connectors.tidb.source;

import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.debezium.event.DebeziumSchemaDataTypeInference;
import org.apache.kafka.connect.data.Schema;

public class TiDBSchemaDataTypeInference extends DebeziumSchemaDataTypeInference {
    private static final long serialVersionUID = 1L;

    protected DataType inferStruct(Object value, Schema schema) {
        // the Geometry datatype in MySQL will be converted to
        // a String with Json format
        if (Point.LOGICAL_NAME.equals(schema.name())
                || Geometry.LOGICAL_NAME.equals(schema.name())) {
            return DataTypes.STRING();
        } else {
            return super.inferStruct(value, schema);
        }
    }
}
