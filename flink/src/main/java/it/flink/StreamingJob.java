package it.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Map;

public class StreamingJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Map<String, Object>> source = KafkaSource.<Map<String, Object>>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("gc-batches")
            .setGroupId("flink-consumer")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new MsgPackDeserializationSchema())
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .map(record -> "Batch id=" + record.get("batch_id") + ", layer=" + record.get("layer") +
                    ", tile=" + record.get("tile_id") + " [" + record.get("print_id") + "]")
            .print();

        env.execute("Kafka MsgPack Reader");
    }
}
