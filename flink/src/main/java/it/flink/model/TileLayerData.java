package it.flink.model;

import java.util.List;

/** Rappresenta i dati di un singolo tile all'interno di un layer */
public class TileLayerData {
    public final String batchId;
    public final String printId;
    public final String tileId;
    public final int layerId;
    public final int[][] temperatureMatrix; 

    // Conteggio dei punti saturati
    public int saturatedCount;

    // Top 5 outlier formattati per l'output (calcolati da Q2)
    public String p1; public String dp1;
    public String p2; public String dp2;
    public String p3; public String dp3;
    public String p4; public String dp4;
    public String p5; public String dp5;

    // Lista completa degli outlier per il clustering nella Q3
    public List<OutlierPoint> outlierPoints;

    // Lista dei cluster risultanti dal clustering DBSCAN (calcolati da Q3)
    public List<Cluster> clusters;

    // Timestamp
    public long processingStartTime = 0L; 
    public long q1EndTime = 0L;
    public long q2StartTime = 0L;
    public long q2EndTime = 0L;

    // Costruttore di default 
    public TileLayerData(String batchId, String printId, String tileId, int layerId, int[][] temperatureMatrix) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layerId = layerId;
        this.temperatureMatrix = temperatureMatrix;
    }


    // ========================== COSTRUTTORI ===========================

    // Costruttore che contiene la saturazione (output di Q1)
    public TileLayerData(String batchId, String printId, String tileId, int layerId, int[][] temperatureMatrix, int saturatedCount,
    long processingStartTime, long q1EndTime, long q2StartTime, long q2EndTime) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layerId = layerId;
        this.temperatureMatrix = temperatureMatrix;
        this.saturatedCount = saturatedCount; 
        this.processingStartTime = processingStartTime;
        this.q1EndTime = q1EndTime;
        this.q2StartTime = q2StartTime;
        this.q2EndTime = q2EndTime;
    }

    // Costruttore che contiene saturazione (output di Q1) e outlierPoints (output di Q2)
    public TileLayerData(String batchId, String printId, String tileId, int layerId, int[][] temperatureMatrix, int saturatedCount, long processingStartTime,
                        long q1EndTime, long q2StartTime, long q2EndTime,
                        String p1, String dp1, String p2, String dp2,
                        String p3, String dp3, String p4, String dp4,
                        String p5, String dp5,
                        List<OutlierPoint> outlierPoints) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layerId = layerId;
        this.temperatureMatrix = temperatureMatrix;
        this.saturatedCount = saturatedCount; 
        this.processingStartTime = processingStartTime;
        this.q1EndTime = q1EndTime;
        this.q2StartTime = q2StartTime;
        this.q2EndTime = q2EndTime;
        this.p1 = p1; this.dp1 = dp1;
        this.p2 = p2; this.dp2 = dp2;
        this.p3 = p3; this.dp3 = dp3;
        this.p4 = p4; this.dp4 = dp4;
        this.p5 = p5; this.dp5 = dp5;
        this.outlierPoints = outlierPoints;
    }

    // Costruttore che contiene saturazione (output di Q1), outlierPoints (output di Q2) e clusters (output di Q3)
    public TileLayerData(String batchId, String printId, String tileId, int layerId, int[][] temperatureMatrix, int saturatedCount,
                        long processingStartTime, long q1EndTime, long q2StartTime, long q2EndTime,
                        String p1, String dp1, String p2, String dp2,
                        String p3, String dp3, String p4, String dp4,
                        String p5, String dp5,
                        List<OutlierPoint> outlierPoints,
                        List<Cluster> clusters) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layerId = layerId;
        this.temperatureMatrix = temperatureMatrix;
        this.saturatedCount = saturatedCount; 
        this.processingStartTime = processingStartTime;
        this.q1EndTime = q1EndTime;
        this.q2StartTime = q2StartTime;
        this.q2EndTime = q2EndTime;
        this.p1 = p1; this.dp1 = dp1;
        this.p2 = p2; this.dp2 = dp2;
        this.p3 = p3; this.dp3 = dp3;
        this.p4 = p4; this.dp4 = dp4;
        this.p5 = p5; this.dp5 = dp5;
        this.outlierPoints = outlierPoints;
        this.clusters = clusters;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("INFO:demo_client:Processing layer %d of print %s, tile %s\n", layerId, printId, tileId));
        sb.append(String.format("{'batch_id': %s, 'print_id': '%s', 'tile_id': %s, 'saturated': %d, 'centroids': [\n",
            batchId, printId, tileId, saturatedCount));

        if (clusters != null && !clusters.isEmpty()) {
            for (int i = 0; i < clusters.size(); i++) {
                Cluster c = clusters.get(i);
                sb.append(String.format(" {'x': np.float64(%.15f), 'y': np.float64(%.15f), 'count': %d}",
                    c.getCentroidX(), c.getCentroidY(), c.getCount()));
                if (i < clusters.size() - 1) sb.append(",\n");
            }
        }
        sb.append("]}");
        return sb.toString();
    }


}
