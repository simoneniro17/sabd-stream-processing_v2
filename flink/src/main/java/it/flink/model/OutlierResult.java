package it.flink.model;

import java.util.List;

/** Modello per rappresentare il risultato dell'analisi di Q2.
 * Contiene sia i dati formattati per l'output CSV che la lista completa degli outlier. */
public class OutlierResult {
    public String batchId;
    public String printId;
    public String tileId;
    
    // Top 5 outlier formattati per l'output (calcolati da Q2)
    public String p1; public String dp1;
    public String p2; public String dp2;
    public String p3; public String dp3;
    public String p4; public String dp4;
    public String p5; public String dp5;

    // Lista completa degli outlier per il clustering nella Q3
    public List<OutlierPoint> outlierPoints;

    public OutlierResult() {}
    
    public OutlierResult(String batchId, String printId, String tileId,
                        String p1, String dp1, String p2, String dp2,
                        String p3, String dp3, String p4, String dp4,
                        String p5, String dp5,
                        List<OutlierPoint> outlierPoints) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.p1 = p1; this.dp1 = dp1;
        this.p2 = p2; this.dp2 = dp2;
        this.p3 = p3; this.dp3 = dp3;
        this.p4 = p4; this.dp4 = dp4;
        this.p5 = p5; this.dp5 = dp5;
        this.outlierPoints = outlierPoints;
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
            batchId, printId, tileId, p1, dp1, p2, dp2, p3, dp3, p4, dp4, p5, dp5);
    }
}