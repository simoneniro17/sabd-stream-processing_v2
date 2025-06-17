package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

import it.flink.model.OutlierOutput;

public class OutlierOutputSerializationSchema implements SerializationSchema<OutlierOutput> {
    @Override
    public byte[] serialize(OutlierOutput outlierOutput) {
        String csv = String.format("%s,%s,%s,%s",
                outlierOutput.batchId,
                outlierOutput.printId,
                outlierOutput.tileId,
                outlierOutput.pointsData);

        return csv.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
