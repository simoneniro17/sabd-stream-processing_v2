package it.flink;

public class SaturationOutput {
    public String batchId;
    public String printId;
    public String tileId;
    public int saturatedCount;

    // Costruttore
    public SaturationOutput(String batchId, String printId, String tileId, int saturatedCount) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.saturatedCount = saturatedCount;
    }

    // per Flink potrebbe essere utile avere anche un costruttore vuoto
    public SaturationOutput() {}
    
    @Override
    public String toString() {
        return String.format("batch_id=%s, print_id=%s, tile_id=%s, saturated=%d", batchId, printId, tileId, saturatedCount);
    }

}
