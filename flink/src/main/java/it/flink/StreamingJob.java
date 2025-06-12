package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        // Crea ambiente Flink
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // KafkaSource con Flink 2.0 API
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("gc-batches")
            .setGroupId("flink-consumer")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        // Aggiungi la sorgente Kafka e stampa i messaggi
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .print();

        // Avvia l'applicazione
        env.execute("Kafka JSON Reader");
    }
}
