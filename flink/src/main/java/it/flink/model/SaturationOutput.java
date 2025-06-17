package it.flink.model;

/** Modello per rappresentare l'output di q1 */
public class SaturationOutput {
    public String batchId;
    public String printId;
    public String tileId;
    public int saturatedCount;

    public SaturationOutput(String batchId, String printId, String tileId, int saturatedCount) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.saturatedCount = saturatedCount;
    }

    // Costruttore vuoto per la serializzazione di Flink
    public SaturationOutput() {}
    

    // TODO: lo possiamo togliere perché mi sa che lo usavamo per debug
    @Override
    public String toString() {
        return String.format("batch_id=%s, print_id=%s, tile_id=%s, saturated=%d", batchId, printId, tileId, saturatedCount);
    }

}
