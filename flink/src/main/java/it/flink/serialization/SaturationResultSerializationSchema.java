package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

import it.flink.model.SaturationResult;

/** Schema di serializzazione per convertire gli oggetti SaturationOutput in formato CSV per Kafka
 * Header CSV atteso: batch_id, print_id, tile_id, saturated */
public class SaturationResultSerializationSchema implements SerializationSchema<SaturationResult>{
    @Override
    public byte[] serialize(SaturationResult saturationOutput) {
        if (saturationOutput == null) {
            return new byte[0];
        }

        String csv = String.format("%s,%s,%s,%d",
                saturationOutput.batchId,
                saturationOutput.printId,
                saturationOutput.tileId,
                saturationOutput.saturatedCount);
        
        return csv.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
