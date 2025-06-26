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
 * per ogni chiave logica ( printId_tileId). Mantiene la finestra nello state store e,
 * ogni volta che la finestra è piena, applica la logica di Query2 e inoltra il risultato.
 */
public class SlidingWindowProcessor implements FixedKeyProcessor<String, TileLayerData, TileLayerData> {

    private KeyValueStore<String, Deque<TileLayerData>> store;
    private FixedKeyProcessorContext<String, TileLayerData> context;

    @Override
    public void init(FixedKeyProcessorContext<String, TileLayerData> context) {
        this.context = context;
        this.store = context.getStateStore("window-store");
    }

    @Override
    public void process(FixedKeyRecord<String, TileLayerData> record) {
        String key = record.key();
        TileLayerData value = record.value();

        Deque<TileLayerData> window = store.get(key);
        if (window == null) window = new LinkedList<>();

        window.addLast(value);
        if (window.size() > 3) window.removeFirst();

        store.put(key, window);

        if (window.size() == 3) {
            TileLayerData output = Query2.analyzeWindow(new LinkedList<>(window));
            context.forward(record.withValue(output));
        }
    }

    @Override
    public void close() {}
}




