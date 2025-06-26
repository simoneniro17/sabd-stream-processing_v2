package it.kafkastreams.utils;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Deque;
import java.util.LinkedList;

import it.kafkastreams.model.TileLayerData;
import it.kafkastreams.processing.Query2;

/**
 * Processor custom per Kafka Streams che implementa una sliding window a conteggio (size 3, slide 1)
 * per ogni chiave logica (printId_tileId). Mantiene la finestra nello state store e,
 * ogni volta che la finestra è piena, applica la logica di Query2 e inoltra il risultato.
 */
public class SlidingWindowProcessor implements FixedKeyProcessor<String, TileLayerData, TileLayerData> {

    private static final int WINDOW_SIZE = 3;
    private static final String STORE_NAME = "window-store";
    
    private KeyValueStore<String, Deque<TileLayerData>> store;
    private FixedKeyProcessorContext<String, TileLayerData> context;

    /** Inizializza il processor con il contesto e recupera il riferimento allo state store */
    @Override
    public void init(FixedKeyProcessorContext<String, TileLayerData> context) {
        this.context = context;
        this.store = context.getStateStore(STORE_NAME);
    }

    /** Processa un record aggiornando la sliding window e applicando l'analisi quando necessario */
    @Override
    public void process(FixedKeyRecord<String, TileLayerData> record) {
        String key = record.key();
        TileLayerData value = record.value();

        // Recupera o crea la finestra per questa chiave
        Deque<TileLayerData> window = store.get(key);
        if (window == null) {
            window = new LinkedList<>();
        }

        // Aggiorna la finestra con il nuovo elemento
        window.addLast(value);
        if (window.size() > WINDOW_SIZE) {
            window.removeFirst();
        }

        // Memorizza la finestra aggiornata
        store.put(key, window);

        // Crea una copia della finestra per l'analisi
        TileLayerData output = Query2.analyzeWindow(new LinkedList<>(window));

        if (output != null) {
            context.forward(record.withValue(output));
        }
    }
    
    @Override
    public void close() {
        // Nessuna risorsa da rilasciare
    }
}