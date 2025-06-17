package it.flink.model;

/** Rappresenta i dati di un singolo tile all'interno di un layer */
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

    @Override
    public String toString() {
        int height = temperatureMatrix.length;
        int width = temperatureMatrix[0].length;
        return String.format("TileLayerData[batch=%s, print=%s, tile=%s, layer=%d, dimensions=%dx%d]", 
            batchId, printId, tileId, layerId, width, height);
    }
}
