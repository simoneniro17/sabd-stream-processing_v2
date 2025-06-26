package it.kafkastreams;

import java.util.Deque;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import it.kafkastreams.model.TileLayerData;
import it.kafkastreams.processing.Query1;
import it.kafkastreams.serialization.DequeSerde;
import it.kafkastreams.serialization.MsgPackKafkaSerde;
import it.kafkastreams.utils.KafkaTopicUtils;
import it.kafkastreams.utils.KafkaWait;
import it.kafkastreams.utils.SlidingWindowProcessor;
import it.kafkastreams.utils.TileLayerExtractor;

/**
 * StreamingJob implementa la logica di processing per l'analisi dei dati tramite Kafka Streams.
 * Gestisce l'estrazione dei dati, l'analisi della saturazione (Query 1) e l'analisi delle sliding windows (Query 2).
 */
public class StreamingJob {
    
    // Configurazione dei topic Kafka
    private static final String INPUT_TOPIC = "gc-batches";
    private static final String KAFKA_BOOTSTRAP_SERVER = "kafka:9092";
    private static final String QUERY1_OUTPUT_TOPIC = "query1-results";
    private static final String QUERY2_OUTPUT_TOPIC = "query2-results";
    private static final String WINDOW_STORE_NAME = "window-store";

    public static void main(String[] args) throws Exception {
        KafkaWait.waitForBroker("kafka", 9092, 1000);
        KafkaTopicUtils.waitForTopic(KAFKA_BOOTSTRAP_SERVER, INPUT_TOPIC, 1000);

        // Configurazione di base di Kafka Streams
        Properties props = getKafkaProperties();
        
        // Creazione del builder per la topologia di streaming e configurazione per le sliding windows
        StreamsBuilder builder = new StreamsBuilder();
        configureStateStore(builder);
        
        // Lettura dal topic di input con deserializzazione MessagePack
        KStream<String, Map<String, Object>> input = builder.stream(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), new MsgPackKafkaSerde())
        );
        
        // Implementazione Query 1
        KStream<String, TileLayerData> analyzedStream = setupQuery1(input);
        
        // Implementazione Query 2
        setupQuery2(analyzedStream);
        
        // Avvio della pipeline Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        // Chiusura controllata dell'applicazione
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
    
    private static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-job-" + System.currentTimeMillis());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MsgPackKafkaSerde.class);

        // Parametri di ottimizzazione per gestire le finestre
        props.put("max.request.size", "2097152");  // 2MB
        props.put("buffer.memory", "33554432");    // 32MB
        props.put("batch.size", "1048576");        // 1MB
        props.put("compression.type", "none");      // Ottimizza la dimensione effettiva
        
        return props;
    }
    
    private static void configureStateStore(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, Deque<TileLayerData>>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(WINDOW_STORE_NAME),
                Serdes.String(),
                new DequeSerde<>(TileLayerData.class)
            );
        builder.addStateStore(storeBuilder);
    }
    
    private static KStream<String, TileLayerData> setupQuery1(KStream<String, Map<String, Object>> input) {
        // Estrazione e analisi saturazione
        KStream<String, TileLayerData> analyzed = input.mapValues(record -> {
            try {
                TileLayerExtractor extractor = new TileLayerExtractor();
                TileLayerData tile = extractor.map(record);
                return Query1.analyzeSaturation(tile);
            } catch (Exception e) {
                System.err.println("Errore nell'estrazione o analisi: " + e.getMessage());
                return null;
            }
        }).filter((k, v) -> v != null);
        
        // Serializza in CSV e pubblica i risultati
        KStream<String, String> csvResults = analyzed.mapValues(tile ->
            String.format("%d,%s,%d,%d", tile.batchId, tile.printId, tile.tileId, tile.saturatedCount)
        );
        
        csvResults.to(QUERY1_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        
        return analyzed;
    }
    
    private static void setupQuery2(KStream<String, TileLayerData> analyzed) {
        // Sliding window di dimensione 3 e sliding di 1
        KStream<String, TileLayerData> processed = analyzed
            .selectKey((k, v) -> v.printId + "_" + v.tileId)
            .processValues(
                () -> new SlidingWindowProcessor(),
                Named.as("query2-processor"),
                WINDOW_STORE_NAME
            );
        
        // Serializza i risultati in CSV
        KStream<String, String> csvResults = processed.mapValues(tile ->
            String.format(
                "%d,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                tile.batchId, tile.printId, tile.tileId,
                tile.p1, tile.dp1,
                tile.p2, tile.dp2,
                tile.p3, tile.dp3,
                tile.p4, tile.dp4,
                tile.p5, tile.dp5
            )
        );
        
        csvResults.to(QUERY2_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}

