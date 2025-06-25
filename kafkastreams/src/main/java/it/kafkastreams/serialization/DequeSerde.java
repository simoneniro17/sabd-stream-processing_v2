package it.kafkastreams.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Deque;
import java.util.LinkedList;

public class DequeSerde<T> implements Serde<Deque<T>> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> clazz;

    public DequeSerde(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Serializer<Deque<T>> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<Deque<T>> deserializer() {
        return (topic, data) -> {
            try {
                return mapper.readValue(data, mapper.getTypeFactory().constructCollectionType(LinkedList.class, clazz));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
