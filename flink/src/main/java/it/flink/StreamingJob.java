package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
import it.flink.utils.KafkaResultPublisher;
import it.flink.utils.KafkaTopicUtils;
import it.flink.utils.KafkaWait;
import it.flink.utils.TileLayerExtractor;

import java.util.Map;

/**
 * Classe principale per l'esecuzione del job di streaming in Flink.
 * Legge dati da Kafka, esegue le query di analisi e scrive i risultati su Kafka.
 */
public class StreamingJob {
    private static final boolean ENABLE_PROFILING = true;       // TRUE per abilitare metriche personalizzate
    private static final String KAFKA_BOOTSTRAP_SERVER = "kafka:9092";
    private static final String INPUT_TOPIC = "gc-batches";
    private static final String SATURATION_OUTPUT_TOPIC = "query1-results";
    private static final String OUTLIER_OUTPUT_TOPIC = "query2-results";
    private static final String CLUSTER_OUTPUT_TOPIC = "query3-results";

    public static void main(String[] args) throws Exception {
        // Set up dell'ambiente di esecuzione di Flink con configurazione per Prometheus (per le metriche personalizzate)
        Configuration config = new Configuration();
        config.setString("metrics.reporter.prom.factory.class", "org.apache.flink.metrics.prometheus.PrometheusReporterFactory");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        // Attesa che Kafka sia pronto e che il topic di input esista
        KafkaWait.waitForBroker("kafka", 9092, 1000);
        KafkaTopicUtils.waitForTopic(KAFKA_BOOTSTRAP_SERVER, INPUT_TOPIC, 1000);

        // env.disableOperatorChaining(); // Utile se vogliamo il DAG non ottimizzato

        // DataStream dalla sorgente Kafka e conversione in un DataStream di TileLayerData
        DataStream<Map<String, Object>> rawStream = createKafkaSource(env);
        DataStream<TileLayerData> tileStream = rawStream
            .map(new TileLayerExtractor())
            .map(tile -> {
                tile.processingStartTime = System.currentTimeMillis();
                return tile;
            }).returns(TileLayerData.class);

        // Esecuzione della pipeline
        tileStream = processQuery1(tileStream);
        tileStream = processQuery2(tileStream);
        processQuery3(tileStream);

        env.execute("StreamingJob");
    }

    /** Crea la sorgente Kafka che legge i dati dal topic specificato */
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
    
    private static DataStream<TileLayerData> processQuery1 (DataStream<TileLayerData> stream) {
        DataStream<TileLayerData> resultStream;

        if (ENABLE_PROFILING) {
            resultStream = stream
                .map(tile -> Query1.analyzeSaturation(tile)).returns(TileLayerData.class)
                .map(new QueryMetrics("query1", MetricType.QUERY1, true));
        } else {
            resultStream = stream
                .map(tile -> Query1.analyzeSaturation(tile)).returns(TileLayerData.class); 
        }

        // Pubblica risultati su Kafka
        new KafkaResultPublisher<>(
            SATURATION_OUTPUT_TOPIC,
            new Query1OutputSerializationSchema(),
            "Q1-sink-"
        ).writeToKafka(resultStream);

        return resultStream;
    }

    private static DataStream<TileLayerData> processQuery2(DataStream<TileLayerData> stream) {
        // Timestamp di inizio processamento Q2
        stream = stream.map(tile -> {
            tile.q2StartTime = System.currentTimeMillis();
            return tile;
        });

        DataStream<TileLayerData> resultStream;

        if (ENABLE_PROFILING) {
            resultStream = stream
                .keyBy(tile -> tile.printId + "_" + tile.tileId)
                .countWindow(3, 1)
                .process(new Query2())
                .map(new QueryMetrics("query2", MetricType.QUERY2, true));
        } else {
            resultStream = stream
                .keyBy(tile -> tile.printId + "_" + tile.tileId)
                .countWindow(3, 1)
                .process(new Query2()); 
        }

        // Pubblica risultati su Kafka
        new KafkaResultPublisher<>(
            OUTLIER_OUTPUT_TOPIC,
            new Query2OutputSerializationSchema(),
            "Q2-sink-"
        ).writeToKafka(resultStream);

        return resultStream;
    }

    private static DataStream<TileLayerData> processQuery3(DataStream<TileLayerData> stream) {
        DataStream<TileLayerData> resultStream = stream.map(new Query3());

        // Pubblica risultati su Kafka
        new KafkaResultPublisher<>(
            CLUSTER_OUTPUT_TOPIC,
            new Query3OutputSerializationSchema(),
            "Q3-sink-"
        ).writeToKafka(resultStream);

        return resultStream;
    }
}