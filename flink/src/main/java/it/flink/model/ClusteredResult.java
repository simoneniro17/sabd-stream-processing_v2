package it.flink.model;

import java.util.List;
import it.flink.processing.Query3.Cluster;

/**
 * Risultato del clustering DBSCAN per una finestra.
 */
public class ClusteredResult {
    public String batchId;
    public String printId;
    public String tileId;
    public List<Cluster> clusters;

    public ClusteredResult(String batchId, String printId, String tileId, List<Cluster> clusters) {
        this.batchId = batchId;
        this.printId = printId;
        this.tileId = tileId;
        this.clusters = clusters;
    }

    @Override
    public String toString() {
        return "ClusteredResult{" +
                "batchId='" + batchId + '\'' +
                ", printId='" + printId + '\'' +
                ", tileId='" + tileId + '\'' +
                ", clustersCount=" + (clusters != null ? clusters.size() : 0) +
                '}';
    }
}

