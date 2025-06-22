package it.kafkastreams;

import java.util.Map;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.kafkastreams.model.TileLayerData;
import it.kafkastreams.processing.Query1;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;
import it.kafkastreams.serialization.MsgPackKafkaSerde;
import it.kafkastreams.serialization.Query1CsvSerializer;
import it.kafkastreams.utils.TileLayerExtractor;
import it.kafkastreams.serialization.Query1CsvSerializer;


public class StreamingJob { public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-job");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Usiamo il nostro MsgPackKafkaSerde per il value
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MsgPackKafkaSerde.class);

        StreamsBuilder builder = new StreamsBuilder();

        //  Lettura dal topic "gc-batches" con deserializzazione MessagePack in Map<String, Object>
        KStream<String, Map<String, Object>> input = builder.stream(
            "gc-batches",
            Consumed.with(Serdes.String(), new MsgPackKafkaSerde())
        );

        // Estrai TileLayerData e applica la query di saturazione
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

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

