package it.flink;

import java.awt.image.BufferedImage;

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
    public static SaturationResult analyzeSaturation(String batchId, String printId, String tileId, BufferedImage img) {
        int saturatedCount = 0;
        
        // Leggiamo pixel con getRaster.getSample 
        for (int y = 0; y < img.getHeight(); y++) {
            for (int x = 0; x < img.getWidth(); x++) {
                int val = img.getRaster().getSample(x, y, 0);
                if (val < 5000) {
                    // Ignora pixel vuoti
                    continue;
                }
                if (val > 65000) {
                    saturatedCount++;
                }
            }
        }
        return new SaturationResult(batchId, printId, tileId, saturatedCount);
    }

}
