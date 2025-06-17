package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

import it.flink.model.SaturationOutput;

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
