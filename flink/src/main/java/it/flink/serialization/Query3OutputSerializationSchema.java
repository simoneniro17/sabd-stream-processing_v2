package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import it.flink.model.TileLayerData;

/** Schema di  serializzazione per convertire gli oggetti OutlierOutput in formato CSV per Kafka
 * Header CSV atteso: seq_id, print_id, tile_id, saturated, centroids */
public class Query3OutputSerializationSchema implements SerializationSchema<TileLayerData> {
    @Override
    public byte[] serialize(TileLayerData tileLayer) {
        if (tileLayer == null) {
            return new byte[0];
        }
        
        String csv = String.format("%s,%s,%s,%s,%s",
                tileLayer.batchId,
                tileLayer.printId,
                tileLayer.tileId,
                tileLayer.saturatedCount,
                tileLayer.clusters.toString());

        return csv.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}