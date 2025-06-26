package it.kafkastreams.model;

/** Rappresenta i dati di un singolo tile all'interno di un layer */
public class TileLayerData {
    public final int batchId;
    public final String printId;
    public final int tileId;
    public final int layerId;
    public final int[][] temperatureMatrix; 

    // Risultati Q1 - Conteggio dei punti saturati
    public int saturatedCount;

    // Risultati Q2 - Top 5 outlier e lista outlier
    public String p1, dp1, p2, dp2, p3, dp3, p4, dp4, p5, dp5;

    /** Costruttre base per i dati iniziali di un tile */
    public TileLayerData(int batchId, String printId, int tileId, int layerId, int[][] temperatureMatrix) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layerId = layerId;
        this.temperatureMatrix = temperatureMatrix;
    }

    /** Aggiunge i risultati di Q1 */
    public void addSaturationResults(int saturatedCount) {
        this.saturatedCount = saturatedCount;
    }

    /** Costruttore vuoto richiesto da Jackson per la deserializzazione */
    public TileLayerData() {
        this.batchId = 0;
        this.printId = "";
        this.tileId = 0;
        this.layerId = 0;
        this.temperatureMatrix = null;
    }

    /** Aggiunge i risultati di Q2 */
    public void addOutlierResults(
        String p1, String dp1, String p2, String dp2,
        String p3, String dp3, String p4, String dp4,
        String p5, String dp5) {

        this.p1 = p1; this.dp1 = dp1;
        this.p2 = p2; this.dp2 = dp2;
        this.p3 = p3; this.dp3 = dp3;
        this.p4 = p4; this.dp4 = dp4;
        this.p5 = p5; this.dp5 = dp5;
    }

  
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TileLayerData{");
        sb.append("printId='" + printId + "', ");
        sb.append("tileId='" + tileId + "', ");
        sb.append("layerId=" + layerId + ", ");
        sb.append("saturatedCount=" + saturatedCount);
        
        // Info sugli outlier da Q2 se presenti
        sb.append(", topOutliers=[");
        if (p1 != null) sb.append(p1 + ":" + dp1);
        if (p2 != null) sb.append(", " + p2 + ":" + dp2);
        if (p3 != null) sb.append(", " + p3 + ":" + dp3);
        if (p4 != null) sb.append(", " + p4 + ":" + dp4);
        if (p5 != null) sb.append(", " + p5 + ":" + dp5);
        sb.append("]");
        
        
        sb.append("}");
        return sb.toString();
    }
}