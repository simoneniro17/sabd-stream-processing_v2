package it.flink.model;

public class TileLayerData {
    public final String batchId;
    public final String printId;
    public final String tileId;
    public final int layerId;
    public final int[][] temperatureMatrix; 

    public TileLayerData(String batchId, String printId, String tileId, int layerId, int[][] temperatureMatrix) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.layerId = layerId;
        this.temperatureMatrix = temperatureMatrix;
    }
}
