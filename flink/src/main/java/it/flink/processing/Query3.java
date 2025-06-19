package it.flink.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;

import it.flink.model.OutlierPoint;
import it.flink.model.TileLayerData;
import it.flink.model.Cluster;
import smile.clustering.DBSCAN;
import smile.math.distance.Distance;

public class Query3 implements MapFunction<TileLayerData, TileLayerData> {
    
    private final double EPSILON = 20;
    private final int MINPTS = 5;

    @Override
    public TileLayerData map(TileLayerData tileLayerStream) throws Exception {
        List<OutlierPoint> points = tileLayerStream.outlierPoints;

        if (points == null || points.isEmpty()) {
            // Nessun punto, ritorna risultato vuoto
            return new TileLayerData(
                tileLayerStream.batchId, 
                tileLayerStream.printId, 
                tileLayerStream.tileId, 
                tileLayerStream.layerId, 
                tileLayerStream.temperatureMatrix, 
                tileLayerStream.saturatedCount,
                tileLayerStream.p1, tileLayerStream.dp1,
                tileLayerStream.p2, tileLayerStream.dp2,
                tileLayerStream.p3, tileLayerStream.dp3,
                tileLayerStream.p4, tileLayerStream.dp4,
                tileLayerStream.p5, tileLayerStream.dp5,
                points,
                Collections.emptyList() // nessun cluster
        );
        }

        // Prepara dati per DBSCAN (coordinate x,y)
        double[][] data = new double[points.size()][2];
        for (int i = 0; i < points.size(); i++) {
            OutlierPoint p = points.get(i);
            data[i][0] = p.x;
            data[i][1] = p.y;
        }

        // Definisce la distanza Euclidea per DBSCAN
        Distance<double[]> dist = (a, b) -> {
            double dx = a[0] - b[0];
            double dy = a[1] - b[1];
            return Math.sqrt(dx * dx + dy * dy);
        };

        // Esegui DBSCAN sui dati
        DBSCAN<double[]> dbscan = DBSCAN.fit(data, dist, MINPTS, EPSILON);


        int [] labels=dbscan.y;
        // Ottieni etichette cluster per ogni punto
        // int[] labels = new int[data.length];
        // for (int i = 0; i < data.length; i++) {
        //     labels[i] = dbscan.predict(data[i]);
        // }

        // Raggruppa i punti per cluster escludendo i rumori (-1)
        Map<Integer, List<OutlierPoint>> clustersMap = new HashMap<>();
        for (int i = 0; i < labels.length; i++) {
            int label = labels[i];
            if (label == -1) continue; // escludi rumore
            clustersMap.computeIfAbsent(label, k -> new ArrayList<>()).add(points.get(i));
        }

        List<Cluster> clusters = new ArrayList<>();
        for (List<OutlierPoint> pts : clustersMap.values()) {

            // andiamo a filtrare i  cluster troppo piccoli (meno di MINPTS punti) per evitare rumore o gruppi non significativi
            if (pts.size() <= MINPTS) continue;
            clusters.add(new Cluster(pts));
}

        // Ritorna il risultato raggruppato
        return new TileLayerData(
            tileLayerStream.batchId, 
            tileLayerStream.printId, 
            tileLayerStream.tileId, 
            tileLayerStream.layerId, 
            tileLayerStream.temperatureMatrix, 
            tileLayerStream.saturatedCount,
            tileLayerStream.p1, tileLayerStream.dp1,
            tileLayerStream.p2, tileLayerStream.dp2,
            tileLayerStream.p3, tileLayerStream.dp3,
            tileLayerStream.p4, tileLayerStream.dp4,
            tileLayerStream.p5, tileLayerStream.dp5,
            points,
            clusters
        );
    }

}
