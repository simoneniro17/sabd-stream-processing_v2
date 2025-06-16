package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Map;

public class StreamingJob {
    public static void main(String[] args) throws Exception {

        // Definiamo l'ambiente di esecuzione
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO: se si vuole eseguire in parallelo su più task manager, impostare il parallelismo

        // Definiamo la sorgente Kafka
        KafkaSource<Map<String, Object>> source = KafkaSource.<Map<String, Object>>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("gc-batches")
            .setGroupId("flink-consumer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new MsgPackDeserializationSchema())   // Deserializer personaliizzatoo
            .build();

        //  Creiamo il DataStream dalla sorgente Kafka
        DataStream<Map<String, Object>> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Convertiamo il DataStream originale da Kafka in un DataStream di TileLayerData con la map personalizzata
        DataStream<TileLayerData> tileLayerStream = kafkaStream.map(new KafkaMapFunction());

        // === Query 1 ===
        DataStream<SaturatedPointCalculation.SaturationResult> saturationResultsStream = tileLayerStream
            .map(tile -> SaturatedPointCalculation.analyzeSaturation(tile));

        DataStream<TileLayerData> tileStream = saturationResultsStream
        .map(result -> result.tile);

        DataStream<SaturationOutput> outputStream = saturationResultsStream
            .map(result -> new SaturationOutput(
                result.tile.batchId,
                result.tile.printId,
                result.tile.tileId,
                result.saturatedCount
            ))
            .returns(SaturationOutput.class);

        KafkaOutputProcessor<SaturationOutput> saturationOutputProcessor = 
            new KafkaOutputProcessor<>("saturation-results-topic", new SaturationOutputSerializationSchema());

        saturationOutputProcessor.writeToKafka(outputStream);

        // === Query 2 ===
        DataStream<OutlierOutput> windowedStream = tileStream
            .keyBy(tile -> tile.printId + "_" + tile.tileId)
            .countWindow(3, 1)
            .process(new OutlierDetection());

        // Output Query 2 (stampa di debug)
        windowedStream.print("Query 2 - Window");

        // Output Query 2 (scrittura su Kafka)
        KafkaOutputProcessor<OutlierOutput> outlierOutputProcessor =
            new KafkaOutputProcessor<>("outlier-results-topic", new OutlierOutputSerializationSchema());

        outlierOutputProcessor.writeToKafka(windowedStream);

        // Esecuzione del job
        env.execute("StreamingJob - Query 1 + Query 2");

    }
}