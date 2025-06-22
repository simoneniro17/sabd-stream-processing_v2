package it.flink.processing;

import it.flink.model.TileLayerData;

/**
 * Implementazione query 1 per identificare i punti saturati in un'immagine TIFF 16 bit.
 * Analizza un tile di un layer e conta i punti con temperatura superiore alla soglia di saturazione.
 */
public class Query1 {
    private static final int SATURATED_THRESHOLD = 65000; // Soglia per punti saturati

    public static TileLayerData analyzeSaturation(TileLayerData tile) {
        if (tile == null || tile.temperatureMatrix == null) {
            throw new IllegalArgumentException("TileLayerData o matrice di temperatura non valida");
        }

        int saturatedCount = 0;
        int[][] matrix = tile.temperatureMatrix;
        int height = matrix.length;
        int width = matrix[0].length;

        // Scansione matrice di temperature
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
