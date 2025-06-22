package it.flink.utils;

import org.apache.flink.api.common.functions.MapFunction;

import it.flink.model.TileLayerData;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

/**
 * Trasforma i record ricevuti da Kafka (Map<String, Object>) in oggetti TileLayerData.
 * Si occupa di estrarre i campi, decodificare l'immagine TIFF e convertirla in una matrice di temperature.
 */
public class TileLayerExtractor implements MapFunction<Map<String, Object>, TileLayerData> {
    
    @Override
    public TileLayerData map(Map<String, Object> record) throws Exception {
        // Estrazione parametri del tile
        int batchId = Integer.parseInt(extractField(record, "batch_id").toString());
        String printId = extractField(record, "print_id").toString();
        int tileId = Integer.parseInt(extractField(record, "tile_id").toString());
        int layerId = Integer.parseInt(extractField(record, "layer").toString());

        // Estrazione e decodifica dellìimmagine TIFF
        byte[] tiffBytes = extractTiffData(extractField(record,  "tif"));
        BufferedImage img = decodeTiffImage(tiffBytes);
        int[][] temperatureMatrix = createTemperatureMatrix(img);
        
        return new TileLayerData(batchId, printId, tileId, layerId, temperatureMatrix);
    }

    /** Estrae un campo dal record e verifica che non sia nullo */
    private Object extractField(Map<String, Object> record, String fieldName) {
        Object value = record.get(fieldName);
        if (value == null) {
            throw new IllegalArgumentException("Campo obbligatorio mancante: " + fieldName);
        }
        return value;
    }

    /** Estre i dati TIFF dal formato ricevuto */
    private byte[] extractTiffData(Object rawTiffData) throws IOException {
        if (rawTiffData instanceof byte[]) {
            return (byte[]) rawTiffData;
        }
        
        if (rawTiffData instanceof ByteBuffer) {
            ByteBuffer buf = (ByteBuffer) rawTiffData;
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }
        
        if (rawTiffData instanceof String) {
            try {
                return Base64.getDecoder().decode((String) rawTiffData);
            } catch (IllegalArgumentException e) {
                throw new IOException("Errore nella decodifica Base64 dei dati TIFF: " + rawTiffData, e);
            }
        }
        
        throw new RuntimeException("Formato dati TIFF non supportato: " + 
                                   (rawTiffData != null ? rawTiffData.getClass().getName() : "null"));
    }

    /** Decodifica l'immagine TIFF */
    private BufferedImage decodeTiffImage(byte[] tiffBytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(tiffBytes)) {
            BufferedImage img = ImageIO.read(bais);
            if (img == null) {
                throw new IOException("Immagine TIFF non valida o non supportata.");
            }
            return img;
        }
    }

    /** Crea una matrice di temperatura per l'immagine passata come input */
    private int[][] createTemperatureMatrix(BufferedImage img) {
        int width = img.getWidth();
        int height = img.getHeight();
        int[][] temperatureMatrix = new int[height][width];

        // Assumiamo che l'immagine sia in scala di grigi e creiamo una matrice di temperature
        // La matrice è organizzata come [y][x], dove y è la riga e x è la colonna

        // è un po' controintuitivo, ma praticamente nelle coordinate dell'immaagine (x,y)
        // avere (0,1) significa spostarsi di 1 verso il basso, non verso destra come ci si aspetterebbe
        // quando ci muoviamo in una matrice

        // per questo la matrice la creiamo di dimensione [height][width], perché height è l'altezza dell'immagine
        // (che corrisponde al numero di righe) e width è la larghezza dell'immagine (che corrisponde al numero di colonne)

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                temperatureMatrix[y][x] = img.getRaster().getSample(x, y, 0);
            }
        }
        return temperatureMatrix;
    }
}