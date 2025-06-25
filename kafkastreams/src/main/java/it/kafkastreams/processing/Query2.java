package it.kafkastreams.processing;

import java.util.Deque;

import it.kafkastreams.model.TileLayerData;

public class Query2 {
        public static TileLayerData analyzeWindow(Deque<TileLayerData> window) {
        // Esempio: identifica un outlier nella serie
        TileLayerData[] tiles = window.toArray(new TileLayerData[0]);
        TileLayerData current = tiles[2];
        return current;
    }
}
