package it.flink.serialization;

import it.flink.model.TileLayerData;

/** Schema di  serializzazione per convertire i risultati di Query 2 in formato CSV per Kafka
 * Header CSV atteso: seq_id, print_id, tile_id, P1, dP1, P2, dP2, P3, dP3, P4, dP4, P5, dP5 */
public class Query2OutputSerializationSchema extends OutputSerializationSchema {
    @Override
    protected String formatCsv(TileLayerData tile) {
        return String.format("%d,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                tile.batchId,
                tile.printId,
                tile.tileId,
                tile.p1, tile.dp1,
                tile.p2, tile.dp2,
                tile.p3, tile.dp3,
                tile.p4, tile.dp4,
                tile.p5, tile.dp5);
    }
}