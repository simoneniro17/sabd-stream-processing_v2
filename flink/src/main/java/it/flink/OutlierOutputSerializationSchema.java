package it.flink;

import org.apache.flink.api.common.serialization.SerializationSchema;

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
