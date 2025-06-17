package it.flink;

import java.util.List;

public class Outlier {
    public String batchId;
    public String printId;
    public String tileId;
    public String pointsData; // Contiene la parte P1, dP1, P2, dP2 e così via
    public List<OutlierPoint> outlierPoints; // Lista dei punti, serv poi per il clustering
    
    public Outlier(String batchId, String printId, String tileId, String pointsData, List<OutlierPoint> outlierPoints) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.pointsData = pointsData;
        this.outlierPoints = outlierPoints;
    }

    // Costruttore vuoto utile per Flink
    public Outlier() {
        // Necessario per la serializzazione/deserializzazione
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s", batchId, printId, tileId, pointsData);
    }
}
