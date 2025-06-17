package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

import it.flink.model.SaturationOutput;

/** Schema di serializzazione per convertire gli oggetti SaturationOutput in formato CSV per Kafka
 * Header CSV atteso: batch_id, print_id, tile_id, saturated */
public class SaturationOutputSerializationSchema implements SerializationSchema<SaturationOutput>{
    @Override
    public byte[] serialize(SaturationOutput saturationOutput) {
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
