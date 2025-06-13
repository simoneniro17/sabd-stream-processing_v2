package it.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

//questa è una funzione di prova solo per vedere se le finestre funzionano correttamente
// e per vedere se i layer scorrono correttamente
public class DummyWindowFunction extends ProcessWindowFunction<TileLayerData, String, String, GlobalWindow> {

    @Override
    public void process(String key, Context context, Iterable<TileLayerData> elements, Collector<String> out) throws Exception {
        List<TileLayerData> layers = new ArrayList<>();
        for (TileLayerData layer : elements) {
            layers.add(layer);
        }

        // Ordina i layer per layerId (così vedi che scorrono)
        layers.sort(Comparator.comparingInt(l -> l.layerId));

        // Stampa il contenuto della finestra
        StringBuilder sb = new StringBuilder();
        sb.append("KEY=").append(key)
          .append(", WINDOW SIZE=").append(layers.size())
          .append(", LAYER_IDS=[");
        for (TileLayerData l : layers) {
            sb.append(l.layerId).append(", ");
        }
        sb.append("]");

        out.collect(sb.toString());
    }
}
