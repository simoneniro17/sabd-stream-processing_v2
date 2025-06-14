package it.flink;


public class SaturatedPointCalculation {
    public static class SaturationResult {
        public final String batchId;
        public final String printId;
        public final String tileId;
        public final int saturatedCount;

        public SaturationResult(String batchId, String printId, String tileId, int saturatedCount) {
            this.batchId = batchId;
            this.printId = printId;
            this.tileId = tileId;
            this.saturatedCount = saturatedCount;
        }

        @Override
        public String toString() {
            return String.format("batch_id=%s, print_id=%s, tile_id=%s, saturated=%d",
                    batchId, printId, tileId, saturatedCount);
        }
    }

    /**
     * Conta i pixel saturati in una immagine TIFF 16 bit.
     * - Esclude pixel < 5000
     * - Conta pixel > 65000
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
                        continue;
                    }
                    if (val > 65000) {
                        matrix[y][x] = -1; // -1 è un valore sentinella che indica di non utilizzare il punto in quanto è saturato
                        saturatedCount++;
                    } 
        }

    }
        return new SaturationResult(tile.batchId, tile.printId, tile.tileId, saturatedCount);
    }
}
