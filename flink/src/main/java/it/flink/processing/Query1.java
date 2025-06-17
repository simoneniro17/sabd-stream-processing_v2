package it.flink.processing;

import it.flink.model.TileLayerData;
import it.flink.model.SaturationResult;


/**
 * Implementazione query 1 per idnetificare i punti saturati in un'immagine TIFF 16 bit.
 */
public class Query1 {
    private static final int EMPTY_THRESHOLD = 5000; // Soglia per aree vuote
    private static final int SATURATED_THRESHOLD = 65000; // Soglia per punti saturati
    private static final int SENTINEL_VALUE = -1; // Valore sentinella per punti non utilizzabili

    /**
     * Identifica punti critici per temperatura:
     * - < 5000: aree vuote (da escludere dalle analisi successive)
     * - > 65000: punti saturati (da conteggiare ed escludere dalle analisi successive)
     */
    public static SaturationResult analyzeSaturation(TileLayerData tile) {
        int saturatedCount = 0;
        int[][] matrix = tile.temperatureMatrix;
        int height = matrix.length;
        int width = matrix[0].length;

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int temperature = matrix[y][x];

                if (temperature < EMPTY_THRESHOLD ||  temperature > SATURATED_THRESHOLD) {
                    // Marchiamo il punto così lo escludiamo dalle analisi successive
                    matrix[y][x] = SENTINEL_VALUE;

                    // e se è un punto saturo lo contiamo
                    if (temperature > SATURATED_THRESHOLD) {
                        saturatedCount++;
                    }
                }
            }
        }
        return new SaturationResult(tile, saturatedCount);
    }
}
