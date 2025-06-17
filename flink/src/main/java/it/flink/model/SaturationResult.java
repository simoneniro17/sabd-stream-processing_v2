package it.flink.model;

/** Modello per rappresentare l'output di q1 */
public class SaturationResult {
    public TileLayerData tile;  // Per l'elaborazione
    public String batchId;      // Per output
    public String printId;      // Per output
    public String tileId;       // Per output
    public int saturatedCount;  // Per entrambi    

    /** Costruttore per uso interno durante l'elaborazione */
    public SaturationResult(TileLayerData tile, int saturatedCount) {
        this.tile = tile;
        this.batchId = tile.batchId;
        this.printId = tile.printId;
        this.tileId = tile.tileId;
        this.saturatedCount = saturatedCount;
    }

    /** Costruttore per l'output finale */
    public SaturationResult(String batchId, String printId, String tileId, int saturatedCount) {
        this.tile = null; // Non necessario per l'output finale
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.saturatedCount = saturatedCount;
    }

    // Costruttore vuoto per la serializzazione di Flink
    public SaturationResult() {}
    

    // TODO: lo possiamo togliere perché mi sa che lo usavamo per debug
    @Override
    public String toString() {
        return String.format("batch_id=%s, print_id=%s, tile_id=%s, saturated=%d", batchId, printId, tileId, saturatedCount);
    }

}
