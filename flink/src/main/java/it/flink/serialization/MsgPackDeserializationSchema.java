package it.flink.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.util.Map;

/** Schema di deserializzazione per i messaggi da Kafka */
public class MsgPackDeserializationSchema implements DeserializationSchema<Map<String, Object>> {

    private static final ObjectMapper mapper = new ObjectMapper(new MessagePackFactory());

    @Override
    public Map<String, Object> deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null; // Gestione del caso in cui il messaggio sia vuoto
        }
        
        // Utilizziamo l'ObjectMapper per deserializzare il messaggio in una mappa
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
