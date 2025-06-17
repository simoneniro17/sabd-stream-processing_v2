package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.Collector;

import it.flink.model.Outlier;
import it.flink.model.OutlierOutput;
import it.flink.model.OutlierPoint;
import it.flink.model.SaturationOutput;
import it.flink.model.TileLayerData;
import it.flink.processing.OutlierDetection;
import it.flink.processing.SaturatedPointCalculation;
import it.flink.serialization.MsgPackDeserializationSchema;
import it.flink.serialization.OutlierOutputSerializationSchema;
import it.flink.serialization.SaturationOutputSerializationSchema;
import it.flink.utils.KafkaMapFunction;
import it.flink.utils.KafkaOutputProcessor;

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
            .setStartingOffsets(OffsetsInitializer.earliest()) // prima avevamo latest, ma ora riusciamo a leggere i dati che già erano presenti
            .setValueOnlyDeserializer(new MsgPackDeserializationSchema())   // Deserializer personaliizzatoo
            .build();

        //  Creiamo il DataStream dalla sorgente Kafka
        DataStream<Map<String, Object>> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Convertiamo il DataStream originale da Kafka in un DataStream di TileLayerData con la map personalizzata
        DataStream<TileLayerData> tileLayerStream = kafkaStream.map(new KafkaMapFunction());

        // === Query 1 ===
        DataStream<SaturatedPointCalculation.SaturationResult> saturationResultsStream = tileLayerStream
            .map(tile -> SaturatedPointCalculation.analyzeSaturation(tile));

        // Stream di input per la query2
        DataStream<TileLayerData> tileStream = saturationResultsStream
        .map(result -> result.tile);

        // Stream per l'output della query 1
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
        DataStream<Outlier> windowedStream = tileStream
            .keyBy(tile -> tile.printId + "_" + tile.tileId)
            .countWindow(3, 1)
            .process(new OutlierDetection());

        // Output Query 2 (stampa di debug)
        windowedStream.print("Query 2 - Window");

        // All'interno di outlierOutput abbiamo una lista di OutlierPoint, la flatmap si occupa di estrarre i punti e creare quindi uno stream per il clustering
        DataStream<OutlierPoint> outlierPointStream = windowedStream
            .flatMap((Outlier outlier, Collector<OutlierPoint> out) -> {
                for (OutlierPoint point : outlier.outlierPoints) {
                    out.collect(point);
                }
            })
            .returns(OutlierPoint.class);
        
        // Creiamo lo stream di Output
        DataStream<OutlierOutput> outlierOutputStream = windowedStream
            .map(outlier -> new OutlierOutput(
                outlier.batchId,
                outlier.printId,
                outlier.tileId,
                outlier.p1,
                outlier.dp1,
                outlier.p2,
                outlier.dp2,
                outlier.p3,
                outlier.dp3,
                outlier.p4,
                outlier.dp4,
                outlier.p5,
                outlier.dp5
            ))
            .returns(OutlierOutput.class);

        // Output Query 2 (scrittura su Kafka)
        KafkaOutputProcessor<OutlierOutput> outlierOutputProcessor =
            new KafkaOutputProcessor<>("outlier-results-topic", new OutlierOutputSerializationSchema());

        outlierOutputProcessor.writeToKafka(outlierOutputStream);



        // Esecuzione del job
        env.execute("StreamingJob - Query 1 + Query 2");

    }
}