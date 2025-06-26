package it.kafkastreams.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Serde custom per Kafka Streams che permette di serializzare e deserializzare una Deque<T>
 * Utile per gestire finestre sliding custom (es. SlidingWindowProcessor) in uno State Store.
 */
public class DequeSerde<T> implements Serde<Deque<T>> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> clazz;

    public DequeSerde(Class<T> clazz) {
        this.clazz = clazz;
    }
    
    /** Serializza una Deque<T> in un array di byte, usando JSON + GZIP */
    @Override
    public Serializer<Deque<T>> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            
            try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
                
                mapper.writeValue(gzipStream, data);
                gzipStream.finish();
                return byteStream.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Errore durante la serializzazione della Deque", e);
            }
        };
    }

    /** Deserializza un array di byte (JSON compresso GZIP) in una Deque<T>. */
    @Override
    public Deserializer<Deque<T>> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            
            try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
                 GZIPInputStream gzipStream = new GZIPInputStream(byteStream)) {
                
                return mapper.readValue(gzipStream, 
                       mapper.getTypeFactory().constructCollectionType(LinkedList.class, clazz));
            } catch (IOException e) {
                throw new RuntimeException("Errore durante la deserializzazione in Deque", e);
            }
        };
    }
}
