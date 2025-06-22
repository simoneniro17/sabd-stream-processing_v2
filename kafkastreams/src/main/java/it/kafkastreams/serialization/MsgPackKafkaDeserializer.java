package it.kafkastreams.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import java.util.Map;

/**
 * Deserializer per Kafka Streams che converte un messaggio Kafka serializzato in MessagePack
 * in una Map<String, Object> utilizzando Jackson e MessagePackFactory.
 * Questo permette di lavorare con dati strutturati prodotti da sistemi eterogenei (es. Python).
 */

public class MsgPackKafkaDeserializer implements Deserializer<Map<String, Object>> {
    private final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    @Override
    public Map<String, Object> deserialize(String topic, byte[] data) {
        try {
            if (data == null || data.length == 0) return null;
            return mapper.readValue(data, Map.class);
        } catch (Exception e) {
            System.err.println("Errore nella deserializzazione MessagePack: " + e.getMessage());
            return null;
        }
    }
}
