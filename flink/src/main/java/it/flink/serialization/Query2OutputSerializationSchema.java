package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import it.flink.model.TileLayerData;

/** Schema di  serializzazione per convertire gli oggetti OutlierOutput in formato CSV per Kafka
 * Header CSV atteso: seq_id, print_id, tile_id, P1, dP1, P2, dP2, P3, dP3, P4, dP4, P5, dP5 */
public class Query2OutputSerializationSchema implements SerializationSchema<TileLayerData> {
    @Override
    public byte[] serialize(TileLayerData tileLayer) {
        if (tileLayer == null) {
            return new byte[0];
        }
        
        String csv = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                tileLayer.batchId,
                tileLayer.printId,
                tileLayer.tileId,
                tileLayer.p1, tileLayer.dp1,
                tileLayer.p2, tileLayer.dp2,
                tileLayer.p3, tileLayer.dp3,
                tileLayer.p4, tileLayer.dp4,
                tileLayer.p5, tileLayer.dp5);

        return csv.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}