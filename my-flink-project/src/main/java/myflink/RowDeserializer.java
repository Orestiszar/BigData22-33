package myflink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import java.io.IOException;

public class RowDeserializer implements DeserializationSchema<Row> {

    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        return Utils.getMapper().readValue(bytes, Row.class);
    }

    @Override
    public boolean isEndOfStream(Row row) {
        return false;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}
