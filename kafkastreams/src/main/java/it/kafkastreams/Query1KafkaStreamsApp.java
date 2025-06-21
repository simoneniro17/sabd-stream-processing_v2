package it.kafkastreams;
import org.apache.kafka.streams.KafkaStreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
//da modificare non l'ho fatto perche aspettavo le modifiche del push
public class Query1KafkaStreamsApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "query1-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Legge dal topic di input (sostituisci "gc-batches" con il tuo topic reale)
        KStream<String, String> input = builder.stream("gc-batches");

        // Dummy: passa i dati così come sono e stampa ogni valore processato
        KStream<String, String> output = input.mapValues(value -> {
            System.out.println("[Query1KafkaStreamsApp] Messaggio processato: " + value);
            return value;
        });

        // Scrive sul topic di output (sostituisci "query1-results" con il tuo topic reale)
        output.to("query1-results", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        

        // Shutdown hook per chiusura pulita
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}