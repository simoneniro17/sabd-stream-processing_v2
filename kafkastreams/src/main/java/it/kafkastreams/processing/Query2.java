package it.kafkastreams.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

import it.kafkastreams.model.OutlierPoint;
import it.kafkastreams.model.TileLayerData;

/**
 * Implementazione Query 2: analisi degli outlier su una finestra di 3 layer con sliding di 1.
 * Per ogni punto, si calcola la deviazione di temperatura locale come differenza 
 * tra la temperatura media dei punti vicini prossimi (distanza di Manhattan 0,1,2)
 * e la temperatura media dei punti vicini esterni (distanza di Manhattan 3,4).
 */
public class Query2 {
    // Costanti utili
    private static final int WINDOW_SIZE = 3;
    private static final int EMPTY_THRESHOLD = 5000;
    private static final int SATURATED_THRESHOLD = 65000;
    private static final int OUTLIER_THRESHOLD = 6000;
    private static final int DISTANCE_FACTOR = 2;
    private static final int TOP_OUTLIERS_COUNT = 5;

    public static TileLayerData analyzeWindow(Deque<TileLayerData> window) {
        if (window == null || window.size() < WINDOW_SIZE) {
            return null;
        }

        // Raccogliamo e ordiniamo i layer per ID
        List<TileLayerData> layers = new ArrayList<>(window);
        layers.sort(Comparator.comparingInt(t -> t.layerId));

        // Prendiamo il layer più recente (il terzo della lista)
        TileLayerData currentLayer = layers.get(layers.size() - 1);

        // Cerchiamo gli outlier nella finestra di layer
        List<OutlierPoint> outliers = findOutliers(layers);

        return createOutput(currentLayer, outliers);
    }

    /** Individua gli outlier analizzando la dev di temperatura */
    private static List<OutlierPoint> findOutliers(List<TileLayerData> layers) {
        List<OutlierPoint> outliers = new ArrayList<>();

        // Matrice 3D per rappresentare i layer sovrapposti
        int[][][] temp3d = createTemperature3DMatrix(layers);
        int depth = temp3d.length;
        int height = temp3d[0].length;
        int width = temp3d[0][0].length;

        int[][] current = temp3d[depth - 1];

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int temp = current[y][x];

                if (temp <= EMPTY_THRESHOLD || temp >= SATURATED_THRESHOLD) {
                    continue;
                }

                // Calcolo statistiche vicini per il punto
                NeighborStats stats = calculateNeighborStats(temp3d, depth, y, x);
                double deviation = Math.abs(stats.getCloseAvg() - stats.getDistantAvg());

                if (deviation > OUTLIER_THRESHOLD) {
                    outliers.add(new OutlierPoint(x, y, deviation));
                }
            }
        }

        outliers.sort((o1, o2) -> Double.compare(o2.deviation, o1.deviation));
        return outliers;
    }

    /** Crea una matrice 3D di temperature dai layer */
    private static int[][][] createTemperature3DMatrix(List<TileLayerData> layers) {
        int depth = layers.size();
        int[][][] matrix = new int[depth][][];

        for (int i = 0; i < depth; i++) {
            matrix[i] = layers.get(i).temperatureMatrix;
        }

        return matrix;
    }

    /** Classe ausiliaria per rappresentare le statistiche dei vicini */
    private static class NeighborStats {
        double closeSum = 0;    // Somma delle temperature dei vicini prossimi
        int closeCount = 0;     // Conteggio dei vicini prossimi
        double distantSum = 0;  // Somma delle temperature dei vicini lontani
        int distantCount = 0;   // Conteggio dei vicini lontani

        // Temperatura media vicini prossimi
        double getCloseAvg() {
            return closeCount > 0 ? closeSum / closeCount : 0.0;
        }

        // Temperatura media vicini lontani
        double getDistantAvg() {
            return distantCount > 0 ? distantSum / distantCount : 0.0;
        }
    }

    private static NeighborStats calculateNeighborStats(int[][][] temperature3d, int depth, int y, int x) {
        NeighborStats stats = new NeighborStats();
        int currentDepth = depth - 1;   // Ultimo layer è quello corrente

        // Vicini prossimi in un cubo 3x3x3 (distanza di Manhattan 0,1,2) intorno al punto centrale
        for (int j = -DISTANCE_FACTOR; j <= DISTANCE_FACTOR; j++) { // Scorriamo le righe
            for (int i = -DISTANCE_FACTOR; i <= DISTANCE_FACTOR; i++) { // Scorriamo le colonne
                for (int d = 0; d < depth; d++) {

                    int manhattanDistance = Math.abs(i) + Math.abs(j) + Math.abs(currentDepth - d);
                    if (manhattanDistance <= DISTANCE_FACTOR) {
                        stats.closeSum += getValue(temperature3d, d, y + j, x + i);
                        stats.closeCount++;
                    }

                }
            }
        }

        // Vicini lontani
        for (int j = -2 * DISTANCE_FACTOR; j <= 2 * DISTANCE_FACTOR; j++) { // Scorriamo le righe
            for (int i = -2 * DISTANCE_FACTOR; i <= 2 * DISTANCE_FACTOR; i++) { // Scorriamo le colonne
                for (int d = 0; d < depth; d++) {

                    int manhattanDistance = Math.abs(i) + Math.abs(j) + Math.abs(currentDepth - d);
                    if (manhattanDistance > DISTANCE_FACTOR && manhattanDistance <= 2 * DISTANCE_FACTOR) {
                        stats.distantSum += getValue(temperature3d, d, y + j, x + i);
                        stats.distantCount++;
                    }

                }
            }
        }

        return stats;
    }

    /** Restituisce il valore della temperatura per un dato punto, o 0 se fuori dai limiti */
    private static double getValue(int[][][] image, int d, int y, int x) {
        if (d < 0 || d >= image.length ||
            y < 0 || y >= image[d].length ||
            x < 0 || x >= image[d][y].length) {
            return 0.0;
        }
        return image[d][y][x];
    }

    /** Crea l'output per la Query 2 con i top 5 outlier */
    private static TileLayerData createOutput(TileLayerData currentLayer, List<OutlierPoint> allOutliers) {
        String[] points = new String[TOP_OUTLIERS_COUNT];
        String[] deviations = new String[TOP_OUTLIERS_COUNT];

        // Inizializziamo gli array
        Arrays.fill(points, "()");
        Arrays.fill(deviations, "");

        // Estraiamo i primi 5 outlier (o tutti se ce ne sono meno di 5)
        List<OutlierPoint> topOutliers = allOutliers.size() > TOP_OUTLIERS_COUNT ?
                allOutliers.subList(0, TOP_OUTLIERS_COUNT) : allOutliers;

        // Popoliamo gli array con i dati che abbiamo
        for (int i = 0; i < topOutliers.size(); i++) {
            OutlierPoint point = topOutliers.get(i);
            points[i] = String.format("(%d;%d)", point.y, point.x); // coordinate della matrice
            // points[i] = String.format("(%d;%d)", point.x, point.y); // Se li voglio stampare come coordinate dell'immagine
            deviations[i] = String.format("%.2f", point.deviation);
        }

        currentLayer.addOutlierResults(
            allOutliers,
            points[0], deviations[0],
            points[1], deviations[1],
            points[2], deviations[2],
            points[3], deviations[3],
            points[4], deviations[4]
        );
        
        return currentLayer;
    }
}