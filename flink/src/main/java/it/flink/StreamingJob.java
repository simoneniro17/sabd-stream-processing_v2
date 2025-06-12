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

        DataStream<String> result = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
        .map(new MapFunction<Map<String, Object>, String>() {
        @Override
        // Utilizziamo una map function personalizzata
        public String map(Map<String, Object> record) throws Exception {
            try {
                // Prendiiamo prima il record e lo scomponiamo in oggetti
                Object batchIdObj = record.get("batch_id");
                Object printIdObj = record.get("print_id");
                Object tileIdObj = record.get("tile_id");
                Object raw = record.get("tif");

                // Controlliamo che i campi non siano nulli
                if (batchIdObj == null || printIdObj == null || tileIdObj == null || raw == null) {
                    throw new RuntimeException("Uno o più campi sono nulli: " + record);
                }

                // traformiamo gli oggetti in stringhe
                String batchId = batchIdObj.toString();
                String printId = printIdObj.toString();
                String tileId = tileIdObj.toString();

                // questa parte serve per processare l'immagine TIFF
                // prima dobbiamo capire il tipo di raw (questa parte può essere rimossa lasciando solo il tipo effttivo andnado per esclusione
                // la lascio perché non si sa mai)
                byte[] tiffBytes;
                if (raw instanceof byte[]) {
                    tiffBytes = (byte[]) raw;
                } else if (raw instanceof ByteBuffer) {
                    ByteBuffer buf = (ByteBuffer) raw;
                    tiffBytes = new byte[buf.remaining()];
                    buf.get(tiffBytes);
                } else if (raw instanceof String) {
                    tiffBytes = Base64.getDecoder().decode((String) raw);
                } else {
                    throw new RuntimeException("Tipo tif non riconosciuto: " + raw.getClass());
                }

                // ora che abbiamo i byte dell'immagine, possiamo decodificarla
                BufferedImage img;
                try (ByteArrayInputStream bais = new ByteArrayInputStream(tiffBytes)) {
                    img = ImageIO.read(bais);
                    if (img == null) {
                        throw new RuntimeException("Errore decodifica TIFF: immagine nulla");
                    }
                }

                // ora che l'immagine è stata decodificata possiamo eseguire query1
                SaturatedPointCalculation.SaturationResult saturationResult =  
                        SaturatedPointCalculation.analyzeSaturation(batchId, printId, tileId, img);

                return saturationResult.toString();

            } catch (Exception e) {
                System.err.println("Errore durante il parsing del record: " + e.getMessage());
                e.printStackTrace();
                return "Errore nel record";
            }
        }
    });
        result.print();

        env.execute("Kafka MsgPack + TIFF Decoder");
    }}

//TODO: La map() esegue l’intera logica per ciascuna immagine, subito dopo averla ricevuta e decodificata. questo non so se è l'approccio effettivamente corretto. 
//Bisogna, ad esempio, creare prima il dtastream e far si che query1 processi tutto il datastream? 