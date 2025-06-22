package it.flink.serialization;

import it.flink.model.TileLayerData;

/** Schema di  serializzazione per convertire i risultati di Query 3 in formato CSV per Kafka
 * Header CSV atteso: seq_id, print_id, tile_id, saturated, centroids */
public class Query3OutputSerializationSchema extends OutputSerializationSchema {
    @Override
    protected String formatCsv(TileLayerData tile) {
        return String.format("%d,%s,%d,%s,%s",
                tile.batchId,
                tile.printId,
                tile.tileId,
                tile.saturatedCount,
                tile.clusters.toString());
    }
}