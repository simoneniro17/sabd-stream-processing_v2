package it.flink.utils;

import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class KafkaTopicUtils {

    public static void waitForTopic(String bootstrapServers, String topicName, long delayMillis) throws Exception {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(config)) {
            while (true) {
                Set<String> topics = admin.listTopics().names().get();
                if (topics.contains(topicName)) {
                    System.out.println("[INFO] Topic trovato: " + topicName);
                    return;
                }

                System.out.println("[INFO] Topic '" + topicName + "' non trovato. Riprovo tra " + (delayMillis / 1000) + " secondi...");
                Thread.sleep(delayMillis);
            }
        }
    }
}

