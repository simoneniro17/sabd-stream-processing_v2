package it.flink.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;

import it.flink.model.OutlierPoint;
import it.flink.model.TileLayerData;
import it.flink.utils.CustomDBSCAN;
import it.flink.model.Cluster;

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

        Collections.sort(points, (p1, p2) -> 
            // Ordina per coordinate x, poi y
            Double.compare(p2.deviation, p1.deviation)
        );

        // Prepara dati per DBSCAN (coordinate x,y)
        double[][] data = new double[points.size()][2];
        for (int i = 0; i < points.size(); i++) {
            OutlierPoint p = points.get(i);
            data[i][0] = p.x;
            data[i][1] = p.y;
        }

        // Esegui DBSCAN sui dati
        CustomDBSCAN dbscan = new CustomDBSCAN(EPSILON, MINPTS);
        int [] labels = dbscan.fit(points);

        // Raggruppa i punti per cluster escludendo i rumori
        Map<Integer, List<OutlierPoint>> clustersMap = new HashMap<>();
        for (int i = 0; i < labels.length; i++) {
            int label = labels[i];
            if (label == -1) continue; // escludi rumore
            clustersMap.computeIfAbsent(label, k -> new ArrayList<>()).add(points.get(i));
        }

        List<Cluster> clusters = new ArrayList<>();
        for (List<OutlierPoint> pts : clustersMap.values()) {
            // andiamo a filtrare i  cluster troppo piccoli (meno di MINPTS punti) per evitare rumore o gruppi non significativi
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
