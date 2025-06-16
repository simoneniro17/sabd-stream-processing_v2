package it.flink;

public class SaturatedPointCalculation {
    public static class SaturationResult {
        public final TileLayerData tile;
        public final int saturatedCount;

        public SaturationResult(TileLayerData tile, int saturatedCount) {
            this.tile = tile; 
            this.saturatedCount = saturatedCount;
        }
    }

    /**
     * Identifica i punti critici per temperatura in una immagine TIFF 16 bit.
     * - < 5000 (aree vuote)
     * - > 65000 (punti saturati)
     */
    public static SaturationResult analyzeSaturation(TileLayerData tile) {
        int saturatedCount = 0;
        int[][] matrix = tile.temperatureMatrix;
        int height = matrix.length;
        int width = matrix[0].length;

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int val = matrix[y][x];
                    if (val < 5000) {
                        matrix[y][x] = -1; // -1 è un valore sentinella che indica di non utilizzare il punto in quanto è vuoto
                    }
                    if (val > 65000) {
                        matrix[y][x] = -1; // -1 è un valore sentinella che indica di non utilizzare il punto in quanto è saturato
                        saturatedCount++;
                    } 
            }
        }
        return new SaturationResult(tile, saturatedCount);
    }
}
