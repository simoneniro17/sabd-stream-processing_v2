package it.flink.model;

import java.util.Collections;
import java.util.List;

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
    public List<OutlierPoint> outlierPoints;

    // Risultati Q3 - Clustering
    public List<Cluster> clusters;

    // Timestamp per monitoraggio prestazioni
    public long processingStartTime = 0L; 
    public long q1EndTime = 0L;
    public long q2StartTime = 0L;
    public long q2EndTime = 0L;

    /** Costruttre base per i dati iniziali di un tile */
    public TileLayerData(int batchId, String printId, int tileId, int layerId, int[][] temperatureMatrix) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layerId = layerId;
        this.temperatureMatrix = temperatureMatrix;
        this.outlierPoints = Collections.emptyList();
        this.clusters = Collections.emptyList();
    }

    /** Aggiunge i risultati di Q1 */
    public void addSaturationResults(int saturatedCount) {
        this.saturatedCount = saturatedCount;
        this.q1EndTime = System.currentTimeMillis();
    }

    /** Aggiunge i risultati di Q2 */
    public void addOutlierResults(
        List<OutlierPoint> allOutliers,
        String p1, String dp1, String p2, String dp2,
        String p3, String dp3, String p4, String dp4,
        String p5, String dp5) {

        this.outlierPoints = allOutliers;
        this.p1 = p1; this.dp1 = dp1;
        this.p2 = p2; this.dp2 = dp2;
        this.p3 = p3; this.dp3 = dp3;
        this.p4 = p4; this.dp4 = dp4;
        this.p5 = p5; this.dp5 = dp5;
        this.q2EndTime = System.currentTimeMillis();
    }

    /** Aggiunge i risultati di Q3 */
    public void addClusterResults(List<Cluster> clusters) {
        this.clusters = clusters;
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
        if (outlierPoints != null && !outlierPoints.isEmpty()) {
            sb.append(", outliers=" + outlierPoints.size());
            sb.append(", topOutliers=[");
            if (p1 != null) sb.append(p1 + ":" + dp1);
            if (p2 != null) sb.append(", " + p2 + ":" + dp2);
            if (p3 != null) sb.append(", " + p3 + ":" + dp3);
            if (p4 != null) sb.append(", " + p4 + ":" + dp4);
            if (p5 != null) sb.append(", " + p5 + ":" + dp5);
            sb.append("]");
        }
        
        // Info sui cluster da Q3 se presenti
        if (clusters != null && !clusters.isEmpty()) {
            sb.append(", clusters=" + clusters.size());
            sb.append(", centroids=[");
            for (int i = 0; i < clusters.size(); i++) {
                Cluster c = clusters.get(i);
                sb.append("(x=" + c.getCentroidX() + 
                        ", y=" + c.getCentroidY() + 
                        ", count=" + c.getCount() + ")");
                if (i < clusters.size() - 1) sb.append(", ");
            }
            sb.append("]");
        }
        
        // Info sui tempi di elaborazione se disponibili
        if (processingStartTime > 0) {
            if (q1EndTime > 0) {
                sb.append(", q1Time=" + (q1EndTime - processingStartTime) + "ms");
            }
            if (q2StartTime > 0 && q2EndTime > 0) {
                sb.append(", q2Time=" + (q2EndTime - q2StartTime) + "ms");
            }
            if (q1EndTime > 0 && q2EndTime > 0) {
                sb.append(", totalTime=" + (q2EndTime - processingStartTime) + "ms");
            }
        }
        
        sb.append("}");
        return sb.toString();
    }
}