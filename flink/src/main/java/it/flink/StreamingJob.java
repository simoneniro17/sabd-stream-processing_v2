package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;


import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Map<String, Object>> source = KafkaSource.<Map<String, Object>>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("gc-batches")
            .setGroupId("flink-consumer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new MsgPackDeserializationSchema())
            .build();

          // === UNA SOLA sorgente Kafka ===
        DataStream<Map<String, Object>> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // === UNA SOLA MAP per decodificare il TIFF ===
        DataStream<TileLayerData> tileLayerStream = kafkaStream.map(new KafkaMapFunction());

        // === Query 1 ===
        DataStream<String> saturationResultStream = tileLayerStream.map(new MapFunction<TileLayerData, String>() {
            @Override
            public String map(TileLayerData tile) throws Exception {
                // Ricrea BufferedImage da temperatureMatrix
                int[][] matrix = tile.temperatureMatrix;
                int height = matrix.length;
                int width = matrix[0].length;

                BufferedImage img = new BufferedImage(width, height, BufferedImage.TYPE_USHORT_GRAY);
                for (int y = 0; y < height; y++) {
                    for (int x = 0; x < width; x++) {
                        img.getRaster().setSample(x, y, 0, matrix[y][x]);
                    }
                }

                SaturatedPointCalculation.SaturationResult saturationResult =
                    SaturatedPointCalculation.analyzeSaturation(tile.batchId, tile.printId, tile.tileId, img);

                return saturationResult.toString();
            }
        });

        // Output Query 1
        saturationResultStream.print("Query 1 - Saturation");

        // === Query 2 ===

        //in .process se si vuole usare la classe che permette di vedere lo scorrimento delle finestre passare DummyWindowFunction
        // oppure OutlierDetection per rilevare gli outlier

        DataStream<String> windowedStream = tileLayerStream
            .keyBy(tile -> tile.printId + "_" + tile.tileId)
            .countWindow(3, 1)
            .process(new OutlierDetection());

        // Output Query 2
        windowedStream.print("Query 2 - Window");

        // === SINGLE EXECUTE ===
        env.execute("StreamingJob - Query 1 + Query 2");

    }
}

//TODO: La map() esegue l’intera logica per ciascuna immagine, subito dopo averla ricevuta e decodificata. questo non so se è l'approccio effettivamente corretto. 
//Bisogna, ad esempio, creare prima il dtastream e far si che query1 processi tutto il datastream? 


// - creo un'unica sorgente kafkaStream
// - faccio una sola map (KafkaMapFunction) che decodifica e prepara il TileLayerData
// - poi applico la Query 1 (saturazione) e la Query 2 (outlier detection) su questo stream già elaborato.
// Così evito di leggere due volte da Kafka 