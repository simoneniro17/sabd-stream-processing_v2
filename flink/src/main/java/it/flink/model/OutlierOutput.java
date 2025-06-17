package it.flink.model;


public class OutlierOutput {
    public String batchId;
    public String printId;
    public String tileId;
    public String pointsData; // Contiene la parte P1, dP1, P2, dP2 e così via
    
    public OutlierOutput(String batchId, String printId, String tileId, String pointsData) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.pointsData = pointsData;
    }

    // Costruttore vuoto utile per Flink
    public OutlierOutput() {
        // Necessario per la serializzazione/deserializzazione
    }

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s", batchId, printId, tileId, pointsData);
    }
}