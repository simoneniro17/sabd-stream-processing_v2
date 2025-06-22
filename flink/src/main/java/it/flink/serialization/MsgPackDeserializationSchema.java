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
    private static final TypeInformation<Map<String, Object>> TYPE =
        TypeInformation.of(new TypeHint<Map<String, Object>>() {});

    @Override
    public Map<String, Object> deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null; // Gestione del caso in cui il messaggio sia vuoto
        }
        
         return mapper.readValue(message, 
           new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
    }

    @Override
    public boolean isEndOfStream(Map<String, Object> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Map<String, Object>> getProducedType() {
        return TYPE;
    }
}
