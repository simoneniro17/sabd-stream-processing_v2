package it.flink.utils;

import java.net.InetSocketAddress;
import java.net.Socket;

public class KafkaWait {

    // Attende che il broker Kafka sia raggiungibile su un determinato host e porta.
    public static void waitForBroker(String host, int port, long retryDelay) {
        System.out.printf("[INFO] Attendo Kafka su %s:%d…%n", host, port);
        while (true) {
            try (Socket sock = new Socket()) {
                sock.connect(new InetSocketAddress(host, port), 2000);
                System.out.println("[INFO] Kafka raggiungibile, si parte.");
                return;
            } catch (Exception e) {
                System.out.print(".");
                try { Thread.sleep(retryDelay); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;   // se il processo viene killato
                }
            }
        }
    }
}
