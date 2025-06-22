package it.flink.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import it.flink.model.TileLayerData;

import java.nio.charset.StandardCharsets;

public abstract class OutputSerializationSchema implements SerializationSchema<TileLayerData> {
    @Override
    public byte[] serialize(TileLayerData tile) {
        if (tile == null) {
            return new byte[0];
        }

        String csv = formatCsv(tile);
        return csv.getBytes(StandardCharsets.UTF_8);
    }    

    protected abstract String formatCsv(TileLayerData tile);
}
