package it.flink;

import org.apache.flink.api.common.functions.MapFunction;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;

public class KafkaMapFunction implements MapFunction<Map<String, Object>, TileLayerData> {
//map uguale a quella implementata da lorenzo nella prima query solo che restituisce un TileLayerData invece di una Stringa, secondo me più utile per future elaborazioni
    @Override
    public TileLayerData map(Map<String, Object> record) throws Exception {
        Object batchIdObj = record.get("batch_id");
        Object printIdObj = record.get("print_id");
        Object tileIdObj = record.get("tile_id");
        Object layerIdObj = record.get("layer");
        Object raw = record.get("tif");

        if (batchIdObj == null || printIdObj == null || tileIdObj == null || layerIdObj == null || raw == null) {
            throw new RuntimeException("Uno o più campi sono nulli: " + record);
        }

        String batchId = batchIdObj.toString();
        String printId = printIdObj.toString();
        String tileId = tileIdObj.toString();
        int layerId = Integer.parseInt(layerIdObj.toString());

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

        BufferedImage img;
        try (ByteArrayInputStream bais = new ByteArrayInputStream(tiffBytes)) {
            img = ImageIO.read(bais);
            if (img == null) {
                throw new RuntimeException("Errore decodifica TIFF: immagine nulla");
            }
        }

        int width = img.getWidth();
        int height = img.getHeight();
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
