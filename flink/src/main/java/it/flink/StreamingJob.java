package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import it.flink.metrics.QueryMetrics;
import it.flink.metrics.QueryMetrics.MetricType;
import it.flink.model.TileLayerData;
import it.flink.processing.Query2;
import it.flink.processing.Query3;
import it.flink.processing.Query1;
import it.flink.serialization.MsgPackDeserializationSchema;
import it.flink.serialization.Query2OutputSerializationSchema;
import it.flink.serialization.Query1OutputSerializationSchema;
import it.flink.serialization.Query3OutputSerializationSchema;
import it.flink.utils.TileLayerExtractor;
import it.flink.utils.KafkaResultPublisher;
import it.flink.utils.KafkaTopicUtils;

import java.util.Map;

import it.flink.utils.KafkaWait;


/**
 * Classe principale per l'esecuzione del job di streaming in Flink.
 * Legge dati da Kafka, esegue le query di analisi e scrive i risultati su Kafka.
 */
public class StreamingJob {
    // True per abilitare metriche (se è a false non troviamo le metriche in UI)
    private static final boolean ENABLE_PROFILING = false;

    private static final String KAFKA_BOOTSTRAP_SERVER = "kafka:9092";
    private static final String INPUT_TOPIC = "gc-batches";
    private static final String SATURATION_OUTPUT_TOPIC = "query1-results";
    private static final String OUTLIER_OUTPUT_TOPIC = "query2-results";
    private static final String CLUSTER_OUTPUT_TOPIC = "query3-results";



    public static void main(String[] args) throws Exception {
        // Definiamo l'ambiente di esecuzione
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO: se si vuole eseguire in parallelo su più task manager, impostare il parallelismo
        // env.setParallelism(2);


        KafkaWait.waitForBroker("kafka", 9092, 1000);
        KafkaTopicUtils.waitForTopic(KAFKA_BOOTSTRAP_SERVER, INPUT_TOPIC, 1000);

        // env.disableOperatorChaining(); // Utile se vogliamo il DAG non ottimizzato

        // Configurazione e creazione del DataStream dalla sorgente Kafka
        DataStream<Map<String, Object>> kafkaStream = createKafkaSource(env);

        // Convertiamo il DataStream originale da Kafka in un DataStream di TileLayerData con la map personalizzata
        DataStream<TileLayerData> tileLayerStream = kafkaStream.map(new TileLayerExtractor());

        tileLayerStream = tileLayerStream.map(tile -> {
            tile.processingStartTime = System.currentTimeMillis();
            return tile;
        }).returns(TileLayerData.class);

        // Esecuzione Query 1
        tileLayerStream = processQuery1(tileLayerStream);

        // Esecuzione Query 2
        tileLayerStream = processQuery2(tileLayerStream);

        // Esecuzione Query 3
        tileLayerStream = processQuery3(tileLayerStream);

        // Esecuzione del job
        env.execute("StreamingJob");
    }

    private static DataStream<Map<String, Object>> createKafkaSource(StreamExecutionEnvironment env) {
        KafkaSource<Map<String, Object>> source = KafkaSource.<Map<String, Object>>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
            .setTopics(INPUT_TOPIC)
            .setGroupId("flink-consumer-" + System.currentTimeMillis()) // Gruppo di consumer unico per ogni esecuzione
            .setStartingOffsets(OffsetsInitializer.earliest())  // Con "earliest" leggiamo messaggi nel topic scritti prima dell'avvio del job
            .setValueOnlyDeserializer(new MsgPackDeserializationSchema())   // Deserializer personalizzato
            .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }
    

    private static DataStream<TileLayerData> processQuery1 (DataStream<TileLayerData> tileLayerStream) {
        tileLayerStream = tileLayerStream
            .map(tile -> Query1.analyzeSaturation(tile)).returns(TileLayerData.class)
            .map(new QueryMetrics("Query1", MetricType.QUERY1, ENABLE_PROFILING));

        // Output su Kafka
        new KafkaResultPublisher<>(SATURATION_OUTPUT_TOPIC, new Query1OutputSerializationSchema(), "Q1-sink-")
            .writeToKafka(tileLayerStream);

        return tileLayerStream;
        
    }

    private static DataStream<TileLayerData> processQuery2(DataStream<TileLayerData> tileLayerStream) {
        // Timestamp di inizio Q2
        tileLayerStream = tileLayerStream.map(tile -> {
            tile.q2StartTime = System.currentTimeMillis();
            return tile;
        });

        // Processamento delle finestre scorrevoli
        tileLayerStream = tileLayerStream
            .keyBy(tile -> tile.printId + "_" + tile.tileId)
            .countWindow(3, 1)
            .process(new Query2())
            .map(new QueryMetrics("query2", MetricType.QUERY2, ENABLE_PROFILING));

        // Output Query 2 (scrittura su Kafka)
        new KafkaResultPublisher<>(OUTLIER_OUTPUT_TOPIC, new Query2OutputSerializationSchema(), "Q2-sink-")
             .writeToKafka(tileLayerStream);

        return tileLayerStream;
    }

    private static DataStream<TileLayerData> processQuery3(DataStream<TileLayerData> tileLayerStream) {

        tileLayerStream = tileLayerStream.map(new Query3());

        // Output Query 3 (scrittura su Kafka)
        new KafkaResultPublisher<>(CLUSTER_OUTPUT_TOPIC, new Query3OutputSerializationSchema(), "Q3-sink-")
             .writeToKafka(tileLayerStream);

        return tileLayerStream;
    }
}