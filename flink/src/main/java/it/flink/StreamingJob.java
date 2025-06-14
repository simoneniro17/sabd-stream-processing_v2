package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import it.flink.SaturatedPointCalculation.SaturationResult;

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
        DataStream<TileLayerData> filteredStream = tileLayerStream.map(tile -> {
            SaturatedPointCalculation.analyzeSaturation(tile);
            return tile;
        });
        // TODO: questa operazione è stateless quindi può essere parallelizzata semplicemente. Parallelizziamo su 16 tile con una KeyedProcessFunction

   
        // === Query 2 ===

        //in .process se si vuole usare la classe che permette di vedere lo scorrimento delle finestre passare DummyWindowFunction
        // oppure OutlierDetection per rilevare gli outlier

        DataStream<String> windowedStream = filteredStream
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