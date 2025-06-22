package it.kafkastreams.processing;

import it.kafkastreams.model.TileLayerData;


/**
 * Implementazione query 1 per idnetificare i punti saturati in un'immagine TIFF 16 bit.
 */
public class Query1 {
    private static final int SATURATED_THRESHOLD = 65000; // Soglia per punti saturati

    public static TileLayerData analyzeSaturation(TileLayerData tile) {
        int saturatedCount = 0;
        int[][] matrix = tile.temperatureMatrix;
        int height = matrix.length;
        int width = matrix[0].length;

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int temperature = matrix[y][x];

                if (temperature > SATURATED_THRESHOLD) {
                    saturatedCount++;
                }
            }
        }
        tile.addSaturationResults(saturatedCount);
        return tile;
}
}
