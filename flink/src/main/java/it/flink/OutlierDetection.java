package it.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.*;


public class OutlierDetection extends ProcessWindowFunction<TileLayerData, OutlierOutput, String, GlobalWindow> {

    @Override
    public void process(String key, Context context, Iterable<TileLayerData> elements, Collector<OutlierOutput> out) throws Exception {
        List<TileLayerData> layers = new ArrayList<>();
        for (TileLayerData tile : elements) {
            layers.add(tile);
        }
        //ordiniamo i layer in base all'id che hanno (0,1,2)
        layers.sort(Comparator.comparingInt(t -> t.layerId));

        // STAMPA per capire cosa contiene la finestra
        System.out.print("Window [" + key + "] contains layerIds: ");
        for (TileLayerData tile : layers) {
            System.out.print(tile.layerId + " ");
        }
        System.out.println();

        //qui controlliamo che abbiamo alemno 3 layer per poter calcolare gli outlier
        if (layers.size() < 3) {
            System.out.println("Not enough data to detect outliers for key: " + key + ". Required: 3, Found: " + layers.size());
            return;
        }

        //selezioniamo l'ultimo layer che è quello più recente, prendiamo il 2 che è quello più recente
        TileLayerData mostRecentLayer = layers.get(2);

        //prendiamo le infomrazioni della temperatura
        int[][] matrix = mostRecentLayer.temperatureMatrix;
        int height = matrix.length;
        int width = matrix[0].length;

        List<OutlierPoint> outliers = new ArrayList<>();
        
        //adniamo a iterare su tutti i pixel del layer
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {

                if (matrix[y][x] == -1) {
                    // saltiamo i pixel saturati (-1 valore sentinella)
                    continue;
                }

                //per ogni punto calcoliamo la media dei visini prossimi e lontani, i prossimi sono quelli 
                //chhe hanno distanza di manhattan 0,1,2 e i lontani quelli che più lontani 3,4

                double mediaViciniProssimi = calcolaMediaVicini(x, y, layers, 0, 2);
                double mediaViciniLontani = calcolaMediaVicini(x, y, layers, 2, 4);
                double deviation = Math.abs(mediaViciniProssimi - mediaViciniLontani);

                if (deviation > 6000) {
                    outliers.add(new OutlierPoint(x, y, deviation));
                }
            }
        }

        // Ordina gli outlier per deviazione decrescente
        outliers.sort((o1, o2) -> Double.compare(o2.deviation, o1.deviation));

        // prendiamo i primi 5 outlier
        // Se ci sono meno di 5 outlier, prenderemo solo quelli disponibili
        List<OutlierPoint> top5 = outliers.subList(0, Math.min(5, outliers.size()));

        // Preparazione dei dati CSV per la parte dei punti e deviazioni
        StringBuilder pointsData = new StringBuilder();

        // Inseriamo i top5 punti e le loro deviazioni nel CSV
        int idx = 1;
        for (OutlierPoint p : top5) {
            if (idx > 1) pointsData.append(", ");
            pointsData.append("P").append(idx).append("=(").append(p.x).append(",").append(p.y).append("), dP")
                .append(idx).append("=").append(String.format("%.2f", p.deviation));
            idx++;
        }

        // Se meno di 5 outlier, riempiamo i campi mancanti
        while (idx <= 5) {
            if (idx > 1) pointsData.append(", ");
            pointsData.append("P").append(idx).append("=(), dP").append(idx).append("=");
            idx++;
        }

        // Creiamo l'oggetto di output e lo inviamo al collector
        OutlierOutput output = new OutlierOutput(
                mostRecentLayer.batchId,
                mostRecentLayer.printId,
                mostRecentLayer.tileId,
                pointsData.toString()
        );
        
        out.collect(output);
    }




    //funziona usata per calcolare la distanza di manhattan in 3 dimensioni dove la terza dimensione è il layer
    private double calcolaMediaVicini(int x0, int y0, List<TileLayerData> layers, int dMin, int dMax) {

        int sum = 0;
        int count = 0;

        int height = layers.get(0).temperatureMatrix.length;
        int width = layers.get(0).temperatureMatrix[0].length;
        int numLayers = layers.size();

        // Iteriamo su tutti i 3 layer
        for (int layerIdx = 0; layerIdx < numLayers; layerIdx++) {
            int[][] matrix = layers.get(layerIdx).temperatureMatrix;

            // Iteriamo sui pixel attorno a (x0, y0) in un quadrato di lato 2*dMax+1
            for (int dy = -dMax; dy <= dMax; dy++) {
                for (int dx = -dMax; dx <= dMax; dx++) {
                    int x = x0 + dx;
                    int y = y0 + dy;

                    if (x >= 0 && x < width && y >= 0 && y < height) {
                        // Calcoliamo la distanza di Manhattan considerando il layer come z
                        int dz = Math.abs(numLayers - 1 - layerIdx);
                        
                        int manhattan = Math.abs(dx) + Math.abs(dy) + dz;
                        // Se la distanza è nel range richiesto, includiamo il pixel nella media
                        if (manhattan >= dMin && manhattan <= dMax) {
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
            return sum / (double) count;
        }
    }
}

// output possibili:

//Query 2 - Window> 3599,SI266220200309225433,15, P1=(), dP1=, P2=(), dP2=, P3=(), dP3=, P4=(), dP4=, P5=(), dP5=
//può capitare che i valori siano vuoti questo è perche non è soddisfatta la condizione di deviazione > 6000

//Query 2 - Window> 3562,SI266220200309225433,10, P1=(0,254), dP1=10404.53, P2=(0,339), dP2=10165.98, P3=(0,253), dP3=10133.04, P4=(0,338), dP4=10108.43, P5=(0,255), dP5=10083.14
// dP1=10404.53 significa che c'è una forte differenza tra la media dei suoi vicini prossimi (vicini stretti, distanza ≤2) e quella dei vicini esterni (distanza 3-4) visto che supera la soglia di 6000
// stessa cosa per gli altri punti