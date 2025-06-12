package it.flink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.Map;

public class MsgPackDeserializationSchema implements DeserializationSchema<Map<String, Object>> {

    private static final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    @Override
    public Map<String, Object> deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, Map.class);
    }

    @Override
    public boolean isEndOfStream(Map<String, Object> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Map<String, Object>> getProducedType() {
        return TypeInformation.of(new TypeHint<Map<String, Object>>() {});
    }
}
