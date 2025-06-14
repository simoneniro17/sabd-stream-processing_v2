package it.flink;

import org.apache.flink.api.common.functions.MapFunction;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;


/**
 * KafkaMapFunction trasforma i record grezzi ricevuti da Kafka (Map<String, Object>)
 * in oggetti TileLayerData, che sono più strutturati e facili da usare nelle successive fasi di elaborazione.
 * Questa funzione si occupa di estrarre i campi, decodificare l'immagine TIFF e
 * convertirla in una matrice di temperature.
 */
public class KafkaMapFunction implements MapFunction<Map<String, Object>, TileLayerData> {
    
    @Override
    public TileLayerData map(Map<String, Object> record) throws Exception {
        Object batchIdObj = record.get("batch_id");
        Object printIdObj = record.get("print_id");
        Object tileIdObj = record.get("tile_id");
        Object layerIdObj = record.get("layer");
        Object rawTiffData = record.get("tif");

        if (batchIdObj == null || printIdObj == null || tileIdObj == null || layerIdObj == null || rawTiffData == null) {
            throw new IllegalArgumentException("Uno o più campi obbligatori sono nulli nel record: " + record);
        }

        // Conversione sicura dei campi estratti ai tipi attesi
        // TODO: convertire tutti in INT visto che sono tutti ID?
        String batchId = batchIdObj.toString();
        String printId = printIdObj.toString();
        String tileId = tileIdObj.toString();
        int layerId = Integer.parseInt(layerIdObj.toString());

        // Gestione del campo "tif" che può essere di diversi tipi, a seconda di come è stato serializzato
        byte[] tiffBytes;
        if (rawTiffData instanceof byte[]) {
            tiffBytes = (byte[]) rawTiffData;
        } else if (rawTiffData instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) rawTiffData;
            tiffBytes = new byte[buf.remaining()];
            buf.get(tiffBytes);
        } else if (rawTiffData instanceof String) {     // Assumiamo che sia Base64 encoded se è una Stringa
            try {
                tiffBytes = Base64.getDecoder().decode((String) rawTiffData);
            } catch (IllegalArgumentException e) {
                throw new IOException("Errore nella decodifica Base64 dei dati TIFF: " + rawTiffData, e);
            }
        } else {
            throw new RuntimeException("Tipo tif non riconosciuto: " + rawTiffData.getClass());
        }

        BufferedImage img;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(tiffBytes)) {
            img = ImageIO.read(bais);
            if (img == null) {
                throw new RuntimeException("Errore decodifica TIFF: immagine nulla");
            }
        } catch (IOException e) {
            throw new IOException("Errore di I/O durante la lettura dei byte dell'immagine TIFF.", e);
        }

        // Dimensioni dell'immagine
        int width = img.getWidth();
        int height = img.getHeight();

        // Creiamo e popoliamo la matrice di temperature (si assume che l'immagine sia in scala di grigi)
        int[][] temperatureMatrix = new int[height][width];
        // Popoliamo la matrice con i valori di temperatura
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                temperatureMatrix[y][x] = img.getRaster().getSample(x, y, 0);
            }
        }

        return new TileLayerData(batchId, printId, tileId, layerId, temperatureMatrix);
    }
}
