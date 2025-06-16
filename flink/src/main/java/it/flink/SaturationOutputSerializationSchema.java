package it.flink;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class SaturationOutputSerializationSchema implements SerializationSchema<SaturationOutput>{
    @Override
    public byte[] serialize(SaturationOutput saturationOutput) {
        String csv = String.format("%s,%s,%s,%d",
                saturationOutput.batchId,
                saturationOutput.printId,
                saturationOutput.tileId,
                saturationOutput.saturatedCount);
        return csv.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
