package it.kafkastreams.serialization;

import it.kafkastreams.model.TileLayerData;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import static java.nio.charset.StandardCharsets.UTF_8;
import it.kafkastreams.model.TileLayerData;


/**
 * Serializer per Kafka Streams che converte un oggetto TileLayerData in una riga CSV.
 * Questo permette di scrivere i risultati della Query 1 su un topic Kafka in formato leggibile e facilmente esportabile.
 * Il formato CSV prodotto è: batchId,printId,tileId,saturatedCount
 */

public class Query1CsvSerializer implements Serializer<TileLayerData> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, TileLayerData tile) {
        if (tile == null) return null;
        String csv = String.format("%d,%s,%d,%d",
                tile.batchId,
                tile.printId,
                tile.tileId,
                tile.saturatedCount);
        return csv.getBytes(UTF_8);
    }

    @Override
    public void close() {}
}
