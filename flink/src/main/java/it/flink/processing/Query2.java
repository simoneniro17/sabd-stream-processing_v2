package it.flink.processing;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import it.flink.model.OutlierPoint;
import it.flink.model.TileLayerData;

import java.util.*;

/**
 * Implementazione Query 2: analisi degli outlier su una finestra di 3 layer con sliding di 1.
 * Per ogni punto, si calcola la deviazione di temperatura locale come differenza 
 * tra la temperatura media dei punti vicini prossimi (distanza di Manhattan 0,1,2)
 * e la temperatura media dei punti vicini esterni (distanza di Manhattan 3,4).
 */
public class Query2 extends ProcessWindowFunction<TileLayerData, TileLayerData,String, GlobalWindow> {
    // Costanti utili
    private static final int WINDOW_SIZE = 3;
    private static final int EMPTY_THRESHOLD = 5000;
    private static final int SATURATED_THRESHOLD = 65000;
    private static final int OUTLIER_THRESHOLD = 6000;
    private static final int DISTANCE_FACTOR = 2;
    private static final int TOP_OUTLIERS_COUNT = 5;

    @Override
    public void process(String key, Context context, Iterable<TileLayerData> elements, Collector<TileLayerData> out) throws Exception {
        // Raccogliamo i layer nella finestra
        List<TileLayerData> layers = collectLayers(elements);

        // Verifichiamo che ci siano i tre layer necessari per l'analisi
        if (layers.size() < WINDOW_SIZE) {
            System.out.println("Dati insufficienti per " + key + ". Richiesti: " + WINDOW_SIZE + ", trovati: " + layers.size());

            TileLayerData currentLayer = layers.get(layers.size() - 1);
        
            // Output vuoto
            TileLayerData emptyResult = new TileLayerData(
                currentLayer.batchId,
                currentLayer.printId,
                currentLayer.tileId,
                currentLayer.layerId,
                currentLayer.temperatureMatrix,
                currentLayer.saturatedCount,
                "()", "", "()", "", "()", "", "()", "", "()", "",
                Collections.emptyList() // Lista vuota di outlier
            );

            out.collect(emptyResult);
            return;
        }

        // Prendiamo il layer più recente (la traccia ci dice così)
        TileLayerData currentLayer = layers.get(layers.size() - 1);

        // Cerchiamo gli outlier
        List<OutlierPoint> outliers = findOutliers(layers);

        // Creiamo l'output con i top 5 outlier
        out.collect(createOutput(currentLayer, outliers));
    }

    /** Raccoglie e ordina i layer della finestra */
    private List<TileLayerData> collectLayers(Iterable<TileLayerData> elements) {
        List<TileLayerData> layers = new ArrayList<>();
        for (TileLayerData tile : elements) {
            layers.add(tile);
        }

        // Ordiniamo i layer in base all'id che hanno
        //TODO: ma siamo sicuri che vada bene fare questa cosa?
        layers.sort(Comparator.comparingInt(t -> t.layerId));

        // Log di debug
        System.out.print("Finestra contiene i layer: ");
        layers.forEach(layer -> System.out.print(layer.layerId + " "));
        System.out.println();

        return layers;
    }

    /** Trovi i punti outlier nel layer corrente */
    private List<OutlierPoint> findOutliers(List<TileLayerData> layers) {
        List<OutlierPoint> outliers = new ArrayList<>();

        // Creiamo una matrice 3D per rappresentare l'immagine, visto che abbiamo 3 layer uno sopra l'altro
        int depth = layers.size();
        int[][][] temperature3d = createTemperature3DMatrix(layers); // 3 layer con dimensioni height x width
        int height = temperature3d[0].length; // Altezza dell'immagine, che sarebbe il numero di righe della matrice
        int width = temperature3d[0][0].length; // Larghezza dell'immagine, che sarebbe il numero di colonne della matrice
        
        // Matrice 2D per rappresentare l'ultimo layer
        int[][] currentLayerMatrix = temperature3d[depth - 1];

        // Per ogni punto dell'ultimo layer
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {

                if (currentLayerMatrix[y][x] <= EMPTY_THRESHOLD || currentLayerMatrix[y][x] >= SATURATED_THRESHOLD) {
                    continue;   // Ignoriamo i punti saturati o vuoti
                }

                // Calcoliamo le statistiche dei vicini per il punto corrente
                NeighborStats stats = calculateNeighborStats(temperature3d, depth, y, x);

                // Calcoliamo la deviazione tra le due medie
                double deviation = Math.abs(stats.getCloseAvg() - stats.getDistantAvg());

                // Aggiungiamo il punto agli outlier se la deviazione supera la soglia
                if (deviation > OUTLIER_THRESHOLD) {
                    outliers.add(new OutlierPoint(x, y, deviation)); // SE COORDINATE DI OutlierPoint indicano quelle dell'immagine
                    // outliers.add(new OutlierPoint(y, x, deviation)); // Se le coordinate di OutlierPoint indicano quelle della matrice E NON QUELLA DELL'IMMAGINE
                }
            }
        }

        // Ordiniamo gli outlier per deviazione decrescente
        outliers.sort((o1, o2) -> Double.compare(o2.deviation, o1.deviation));
        return outliers;
    }

    /** Crea una matrice 3D di temperature dai layer */
    private int[][][] createTemperature3DMatrix(List<TileLayerData> layers) {
        int depth = layers.size();
        int[][][] temperature3d = new int[depth][][];

        for (int d = 0; d < depth; d++) {
            temperature3d[d] = layers.get(d).temperatureMatrix;
        }
        return temperature3d;
    }

    /** Classe ausiliaria per rappresentare le statistiche dei vicini */
    private static class NeighborStats {
        double closeSum = 0;
        int closeCount = 0;
        double distantSum = 0;
        int distantCount = 0;

        double getCloseAvg() {
            return closeCount > 0 ? closeSum / closeCount : 0.0;
        }

        double getDistantAvg() {
            return distantCount > 0 ? distantSum / distantCount : 0.0;
        }
    }

    /** Calcola le statistiche dei vicini per un punto specifico */
    private NeighborStats calculateNeighborStats(int[][][] temperature3d, int depth, int y, int x) {
        NeighborStats stats = new NeighborStats();

        // Vicini prossimi
        for (int j = -DISTANCE_FACTOR; j <= DISTANCE_FACTOR; j++) { // Scorriamo le righe
            for (int i = -DISTANCE_FACTOR; i <= DISTANCE_FACTOR; i++) { // Scorriamo le colonne
                for (int d = 0; d < depth; d++) {   // Scorriamo i layer

                    int manhattanDistance = Math.abs(i) + Math.abs(j) + Math.abs(depth - 1 - d);
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
                for (int d = 0; d < depth; d++) {   // Scorriamo i layer

                    int manhattanDistance = Math.abs(i) + Math.abs(j) + Math.abs(depth - 1 - d);
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
    private double getValue(int[][][] image, int d, int y, int x) {
        if(d < 0 || d >= image.length || y < 0 || y >= image[d].length || x < 0 || x >= image[d][y].length) {
            return 0.0;
        }
        return image[d][y][x];
        
    }

    /** Crea l'output per la Query 2 con i top 5 outlier */
    private TileLayerData createOutput(TileLayerData currentLayer, List<OutlierPoint> allOutliers) {
        // Array per i campi di output
        String[] points = new String[TOP_OUTLIERS_COUNT];
        String[] deviations = new String[TOP_OUTLIERS_COUNT];

        // Inizializziamo gli array
        for (int i = 0; i < TOP_OUTLIERS_COUNT; i++) {
            points[i] = "()";
            deviations[i] = "";
        }

        // Estraiamo i primi 5 outlier (o tutti se ce ne sono meno di 5)
        List<OutlierPoint> topOutliers = allOutliers.size() > TOP_OUTLIERS_COUNT ?
                allOutliers.subList(0, TOP_OUTLIERS_COUNT) : allOutliers;

        // Popoliamo gli array con i dati che abbiamo
        for (int i = 0; i < topOutliers.size(); i++) {
            OutlierPoint point = topOutliers.get(i);
            points[i] = String.format("(%d;%d)", point.y, point.x); // Se li voglio stampare come coordinate della matrice
            // points[i] = String.format("(%d;%d)", point.x, point.y); // Se li voglio stampare come coordinate dell'immagine
            deviations[i] = String.format("%.2f", point.deviation);
        }

        return new TileLayerData(
                currentLayer.batchId,
                currentLayer.printId,
                currentLayer.tileId,
                currentLayer.layerId,
                currentLayer.temperatureMatrix,
                currentLayer.saturatedCount,
                points[0], deviations[0],
                points[1], deviations[1],
                points[2], deviations[2],
                points[3], deviations[3],
                points[4], deviations[4],
                allOutliers // Salviamo tutti gli outlier per la Q3
        );
    }
}
