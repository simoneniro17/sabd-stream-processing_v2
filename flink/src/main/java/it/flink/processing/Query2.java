package it.flink.processing;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import it.flink.model.OutlierResult;
import it.flink.model.OutlierPoint;
import it.flink.model.TileLayerData;

import java.util.*;

/**
 * Implementazione Query 2: analisi degli outlier su una finestra di 3 layer con sliding di 1.
 * Per ogni punto, si calcola la deviazione di temperatura locale come differenza 
 * tra la temperatura media dei punti vicini prossimi (distanza di Manhattan 0,1,2)
 * e la temperatura media dei punti vicini esterni (distanza di Manhattan 3,4).
 */
public class Query2 extends ProcessWindowFunction<TileLayerData, OutlierResult, String, GlobalWindow> {
    // Costanti utili
    private static final int WINDOW_SIZE = 3;
    private static final int CLOSE_NEIGHBORS_MIN = 0;
    private static final int CLOSE_NEIGHBORS_MAX = 2;
    private static final int DISTANT_NEIGHBORS_MIN = 3;
    private static final int DISTANT_NEIGHBORS_MAX = 4;
    private static final int OUTLIER_THRESHOLD = 6000;
    private static final int SENTINEL_VALUE = -1;
    private static final int TOP_OUTLIERS_COUNT = 5;

    @Override
    public void process(String key, Context context, Iterable<TileLayerData> elements, Collector<OutlierResult> out) throws Exception {
        // Raccogliamo i layer nella finestra
        List<TileLayerData> layers = collectLayers(elements);

        // Verifichiamo che ci siano i tre layer necessari per l'analisi
        if (layers.size() < WINDOW_SIZE) {
            System.out.println("Dati insufficienti per " + key + ". Richiesti: " + WINDOW_SIZE + ", trovati: " + layers.size());
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
        TileLayerData currentLayer = layers.get(layers.size() - 1);

        int[][] matrix = currentLayer.temperatureMatrix;
        int height = matrix.length;
        int width = matrix[0].length;

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                if (matrix[y][x] == SENTINEL_VALUE) {
                    continue;   // Ignoriamo i punti saturati o vuoti (-1 valore sentinella)
                }

                // Calcoliamo le medie di temperatura dei vicini prossimi e lontani
                double closeNeighborsAvg = calculateNeighborsAvg(x, y, layers, CLOSE_NEIGHBORS_MIN, CLOSE_NEIGHBORS_MAX);
                double distantNeighborsAvg = calculateNeighborsAvg(x, y, layers, DISTANT_NEIGHBORS_MIN, DISTANT_NEIGHBORS_MAX);

                // Calcoliamo la deviazione tra le due medie
                double deviation = Math.abs(closeNeighborsAvg - distantNeighborsAvg);

                // Se la deviazione supera la soglia, aggiungiamo il punto agli outlier
                if (deviation > OUTLIER_THRESHOLD) {
                    outliers.add(new OutlierPoint(x, y, deviation));
                }
            }
        }

        // Ordiniamo gli outlier per deviazione decrescente
        outliers.sort((o1, o2) -> Double.compare(o2.deviation, o1.deviation));
        return outliers;
    }

    /** Calcola la media dei vicini a una distanza specificata dal punto (x,y) */
    private double calculateNeighborsAvg(int x0, int y0, List<TileLayerData> layers, int minDist, int maxDist) {
        int sum = 0;
        int count = 0;

        int height = layers.get(0).temperatureMatrix.length;
        int width = layers.get(0).temperatureMatrix[0].length;
        int numLayers = layers.size();

        // Iteriamo su tutti i 3 layer
        for (int layerIdx = 0; layerIdx < numLayers; layerIdx++) {
            int[][] matrix = layers.get(layerIdx).temperatureMatrix;

            // Iteriamo sui pixel attorno a (x0, y0) in un quadrato di lato 2*dMax+1
            for (int dy = -maxDist; dy <= maxDist; dy++) {
                for (int dx = -maxDist; dx <= maxDist; dx++) {
                    int x = x0 + dx;
                    int y = y0 + dy;

                    // Verifichiamo che il punto sia all'interno della matrice
                    if (x >= 0 && x < width && y >= 0 && y < height) {
                        // Ignoriamo i punti saturati o vuoti (-1 valore sentinella)
                        // In questo modo anche i punti nuovi che raggiungiamo a partire da x0 e y0 muovendoci sulla griglia
                        // non vengono considerati se sono saturati o vuoti
                        if (matrix[y][x] == SENTINEL_VALUE) {
                            continue;
                        }

                        // Calcoliamo la distanza di Manhattan in 3D (il layer sarebbe la nostra z)
                        int dz = Math.abs(numLayers - 1 - layerIdx);
                        int manhattanDistance = Math.abs(dx) + Math.abs(dy) + dz;

                        // Se la distanza è nel range richiesto, includiamo il punto nella media
                        if (manhattanDistance >= minDist && manhattanDistance <= maxDist) {
                            sum += matrix[y][x];
                            count++;
                        }
                    }
                }
            }
        }

        if (count == 0) {
            return 0.0;
        } else {
            return sum / (double)count;
        }
    }

    /** Crea l'output per la Query 2 con i top 5 outlier */
    private OutlierResult createOutput(TileLayerData currentLayer, List<OutlierPoint> allOutliers) {
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
            points[i] = String.format("(%d;%d)", point.x, point.y);
            deviations[i] = String.format("%.2f", point.deviation);
        }

        return new OutlierResult(
                currentLayer.batchId,
                currentLayer.printId,
                currentLayer.tileId,
                points[0], deviations[0],
                points[1], deviations[1],
                points[2], deviations[2],
                points[3], deviations[3],
                points[4], deviations[4],
                allOutliers // Salviamo tutti gli outlier per la Q3
        );
    }
}

// output possibili:

//Query 2 - Window> 3599,SI266220200309225433,15, P1=(), dP1=, P2=(), dP2=, P3=(), dP3=, P4=(), dP4=, P5=(), dP5=
//può capitare che i valori siano vuoti questo è perche non è soddisfatta la condizione di deviazione > 6000

//Query 2 - Window> 3562,SI266220200309225433,10, P1=(0,254), dP1=10404.53, P2=(0,339), dP2=10165.98, P3=(0,253), dP3=10133.04, P4=(0,338), dP4=10108.43, P5=(0,255), dP5=10083.14
// dP1=10404.53 significa che c'è una forte differenza tra la media dei suoi vicini prossimi (vicini stretti, distanza ≤2) e quella dei vicini esterni (distanza 3-4) visto che supera la soglia di 6000
// stessa cosa per gli altri punti