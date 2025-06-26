package it.kafkastreams;

import java.util.Deque;
import java.util.Map;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.kafkastreams.model.TileLayerData;
import it.kafkastreams.processing.Query1;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;
import it.kafkastreams.serialization.MsgPackKafkaSerde;
import it.kafkastreams.serialization.Query1CsvSerializer;
import it.kafkastreams.utils.SlidingWindowProcessor;
import it.kafkastreams.utils.TileLayerExtractor;
import it.kafkastreams.serialization.DequeSerde;
import it.kafkastreams.serialization.Query1CsvSerializer;
import java.util.Deque;


public class StreamingJob { public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-job");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Usiamo il nostro MsgPackKafkaSerde per il value
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MsgPackKafkaSerde.class);

        //parametri di configurazione per gestire le finestre 
        props.put("max.request.size", "2097152"); 
        props.put("buffer.memory", "33554432");   
        props.put("batch.size", "1048576");      
        props.put("compression.type", "lz4");     // AIUTA a ridurre la dimensione effettiva

        StreamsBuilder builder = new StreamsBuilder();

        // State store per la sliding window custom
        StoreBuilder<KeyValueStore<String, Deque<TileLayerData>>> storeBuilder =
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("window-store"),
                Serdes.String(),
                new DequeSerde<>(TileLayerData.class)
            );
        builder.addStateStore(storeBuilder);

        //  Lettura dal topic "gc-batches" con deserializzazione MessagePack in Map<String, Object>
        KStream<String, Map<String, Object>> input = builder.stream(
            "gc-batches",
            Consumed.with(Serdes.String(), new MsgPackKafkaSerde())
        );

        // Estrazione e analisi saturazione (Query 1)
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

        // Serializza in CSV come String
        KStream<String, String> csvResults = analyzed.mapValues(tile ->
            String.format("%d,%s,%d,%d", tile.batchId, tile.printId, tile.tileId, tile.saturatedCount)
        );

        //scrittura dei risultati su un topic Kafka in CSV
        csvResults.to("query1-results", Produced.with(Serdes.String(), Serdes.String()));
      

        // =========================
        // QUERY 2: Sliding window di 3 layer per chiave (printId_tileId)
        // =========================
        KStream<String, TileLayerData> processed = analyzed
        .selectKey((k, v) -> v.printId + "_" + v.tileId)
        .processValues(
            () -> new SlidingWindowProcessor(),
            Named.as("query2-processor"),
            "window-store"
        );
        // Serializza i risultati di Query 2 in CSV
        KStream<String, String> q2CsvResults = processed.mapValues(tile ->
                String.format(
            "%d,%s,%d,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
            tile.batchId, tile.printId, tile.tileId, tile.layerId,
            tile.p1, tile.dp1,
            tile.p2, tile.dp2,
            tile.p3, tile.dp3,
            tile.p4, tile.dp4,
            tile.p5, tile.dp5
        ));

        q2CsvResults.to("query2-results", Produced.with(Serdes.String(), Serdes.String()));

        
        // Avvio della pipeline Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

}
}

