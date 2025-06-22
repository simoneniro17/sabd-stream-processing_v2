package it.flink.serialization;

import it.flink.model.TileLayerData;

/** Schema di serializzazione per i risultati di Query 1 in formato CSV per Kafka.
 * Header CSV atteso: batch_id, print_id, tile_id, saturated */
public class Query1OutputSerializationSchema extends OutputSerializationSchema {
    @Override
    protected String formatCsv(TileLayerData tile) {
        return String.format("%d,%s,%d,%d",
                tile.batchId,
                tile.printId,
                tile.tileId,
                tile.saturatedCount);
    }
}
