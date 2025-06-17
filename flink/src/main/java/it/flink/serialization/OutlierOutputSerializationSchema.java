package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

import it.flink.model.OutlierOutput;

public class OutlierOutputSerializationSchema implements SerializationSchema<OutlierOutput> {
    @Override
    public byte[] serialize(OutlierOutput outlierOutput) {
        // Nuova implementazione che usa i campi individuali p1, dp1, ecc.
        String csv = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                outlierOutput.batchId,
                outlierOutput.printId,
                outlierOutput.tileId,
                outlierOutput.p1,
                outlierOutput.dp1,
                outlierOutput.p2,
                outlierOutput.dp2,
                outlierOutput.p3,
                outlierOutput.dp3,
                outlierOutput.p4,
                outlierOutput.dp4,
                outlierOutput.p5,
                outlierOutput.dp5);

        return csv.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}