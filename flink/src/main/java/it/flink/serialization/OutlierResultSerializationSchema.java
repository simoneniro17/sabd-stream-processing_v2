package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

import it.flink.model.OutlierResult;

/** Schema di  serializzazione per convertire gli oggetti OutlierOutput in formato CSV per Kafka
 * Header CSV atteso: seq_id, print_id, tile_id, P1, dP1, P2, dP2, P3, dP3, P4, dP4, P5, dP5 */
public class OutlierResultSerializationSchema implements SerializationSchema<OutlierResult> {
    @Override
    public byte[] serialize(OutlierResult outlierOutput) {
        if (outlierOutput == null) {
            return new byte[0];
        }
        
        String csv = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                outlierOutput.batchId,
                outlierOutput.printId,
                outlierOutput.tileId,
                outlierOutput.p1, outlierOutput.dp1,
                outlierOutput.p2, outlierOutput.dp2,
                outlierOutput.p3, outlierOutput.dp3,
                outlierOutput.p4, outlierOutput.dp4,
                outlierOutput.p5, outlierOutput.dp5);

        return csv.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}