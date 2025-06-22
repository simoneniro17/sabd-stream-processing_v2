package it.flink.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;

import it.flink.model.Cluster;
import it.flink.model.OutlierPoint;
import it.flink.model.TileLayerData;
import it.flink.utils.CustomDBSCAN;

/** Implementazione Query 3 per il clustering.
 * Raggruppa gli outlier (da Q2) utilizzando DBSCAN e calcola i centroidi dei cluster risultanti
 */
public class Query3 implements MapFunction<TileLayerData, TileLayerData> {
    // Parametri per DBSCAN
    private final double EPSILON = 20;  // Raggio di vicinato
    private final int MIN_POINTS = 5;       // Punti minimi per formare un cluster

    @Override
    public TileLayerData map(TileLayerData tile) throws Exception {
        List<OutlierPoint> points = tile.outlierPoints;

        // Non ci sono outlier e quindi aggiungiamo una lista vuota
        if (points == null || points.isEmpty()) {
            tile.addClusterResults(Collections.emptyList());
            return tile;
        }

        // Ordina gli outlier per deviazione decrescente
        Collections.sort(points, (p1, p2) -> Double.compare(p2.deviation, p1.deviation));

        // DBSCAN sui punti outlier
        CustomDBSCAN dbscan = new CustomDBSCAN(EPSILON, MIN_POINTS);
        int [] labels = dbscan.fit(points);

        // Raggruppa i punti per cluster escludendo i rumori
        Map<Integer, List<OutlierPoint>> clustersMap = new HashMap<>();
        for (int i = 0; i < labels.length; i++) {
            int label = labels[i];
            if (label != -1) {  // Ignoriamo i punti etichettati come rumore
                clustersMap.computeIfAbsent(label, k -> new ArrayList<>()).add(points.get(i));
            }
        }

        // Creiamo i cluster a partire dai punti raggruppati
        List<Cluster> clusters = new ArrayList<>();
        for (List<OutlierPoint> clusterPoints : clustersMap.values()) {
                clusters.add(new Cluster(clusterPoints));
        }

        tile.addClusterResults(clusters);
        return tile;
    }
}
