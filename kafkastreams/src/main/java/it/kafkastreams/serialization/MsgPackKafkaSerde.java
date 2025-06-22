package it.kafkastreams.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * Serde custom per Kafka Streams che permette di deserializzare i messaggi MessagePack in Map<String, Object>.
 * La serializzazione non è implementata (restituisce sempre null) perché in questo caso serve solo la deserializzazione.
 */

public class MsgPackKafkaSerde implements Serde<Map<String, Object>> {
    private final MsgPackKafkaDeserializer deserializer = new MsgPackKafkaDeserializer();

    @Override
    public Serializer<Map<String, Object>> serializer() {
        // Dummy serializer: restituisce sempre null
        return (topic, data) -> null;
    }

    @Override
    public Deserializer<Map<String, Object>> deserializer() {
        return deserializer;
    }
}