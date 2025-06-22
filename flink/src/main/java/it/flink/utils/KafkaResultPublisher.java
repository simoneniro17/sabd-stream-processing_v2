package it.flink.utils;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/** Classe utile per semplificare l'invio dei risultati a Kafka.
 * Incapsula la configurazione e la creazione di un KafkaSink per un tipo generico tipo T. */
public class KafkaResultPublisher<T> {
    private static final String KAFKA_BOOTSTRAP_SERVER = "kafka:9092";
    
    private final KafkaSink<T> kafkaSink;

    /** Crea un publisher per un topic specifico con lo schema di serializzazione fornito */
    public KafkaResultPublisher(String kafkaTopic, SerializationSchema<T> serializer, String TransactionalIdPrefix) {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        // Creazione del sink Kafka
        this.kafkaSink = KafkaSink.<T>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVER)
            .setTransactionalIdPrefix(TransactionalIdPrefix)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(kafkaTopic)
                    .setValueSerializationSchema(serializer)
                    .build()
            )
            .setKafkaProducerConfig(producerConfig)
            .build();
    }

    /** Invia lo stream di dati a Kafka usando il sink configurato */
    public void writeToKafka(DataStream<T> stream) {
        stream.sinkTo(kafkaSink);
    }
}