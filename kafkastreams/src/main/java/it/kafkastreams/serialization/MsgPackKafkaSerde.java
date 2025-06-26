package it.kafkastreams.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serde personalizzato per la deserializzazione di messaggi MessagePack in Map<String, Object>.
 * La serializzazione non è implementata poiché in questo contesto è necessaria solo la deserializzazione.
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