package it.flink.model;

import java.util.List;

/** Rappresenta il risultato dell'analisi degli outlier per un tile */
public class Outlier {
    public String batchId;
    public String printId;
    public String tileId;
    
    // Top 5 outlier formattati per l'output CSV di Q2
    public String p1; public String dp1;
    public String p2; public String dp2;
    public String p3; public String dp3;
    public String p4; public String dp4;
    public String p5; public String dp5;

    // Lista completa degli outlier per il clustering nella Q3
    public List<OutlierPoint> outlierPoints;

    // Costruttore vuoto utile per Flink
    public Outlier() {}

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", 
            batchId, printId, tileId, p1, dp1, p2, dp2, p3, dp3, p4, dp4, p5, dp5);
    }

}
