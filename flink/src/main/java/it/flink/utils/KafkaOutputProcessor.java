package it.flink.utils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaOutputProcessor<T> {

    private final KafkaSink<T> kafkaSink;

    public KafkaOutputProcessor(String kafkaTopic, SerializationSchema<T> serializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        this.kafkaSink = KafkaSink.<T>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(kafkaTopic)
                    .setValueSerializationSchema(serializer)
                    .build()
            )
            .setKafkaProducerConfig(props)
            .build();
    }

    public void writeToKafka(DataStream<T> stream) {
        stream.sinkTo(kafkaSink);
    }
}

