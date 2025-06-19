import argparse
import os
import csv
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition

def main():
    # === PARSING DEGLI ARGOMENTI DA RIGA DI COMANDO ===
    parser = argparse.ArgumentParser(description="Kafka Consumer per salvare i messaggi in CSV")
    parser.add_argument("--topic", required=True, help="Nome del topic Kafka da consumare")
    parser.add_argument("--broker", default="kafka:9092", help="Indirizzo del broker Kafka (default: kafka:9092)")
    args = parser.parse_args()
    topic = args.topic
    broker = args.broker

    # === CREA CARTELLA DI OUTPUT SE NON ESISTE ===
    output_dir = "/results"
    os.makedirs(output_dir, exist_ok=True)

    print(f"[INFO] Connessione a Kafka su '{broker}' e ascolto del topic '{topic}'...")

    # === CONFIGURA IL CONSUMER KAFKA ===
    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        auto_offset_reset='latest',
        enable_auto_commit=False,
        value_deserializer=lambda m: m.decode('utf-8'),
        consumer_timeout_ms=1000
    )

    # === OTTIENI LE PARTIZIONI DEL TOPIC ===
    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        print(f"[ERRORE] Il topic '{topic}' non esiste o è vuoto.")
        return

    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)

    # === CALCOLO DEGLI END OFFSETS ===
    # Gli end offsets rappresentano il "limite superiore": non vogliamo leggere oltre questi,
    # perché significherebbe leggere messaggi che arriveranno DOPO la nostra richiesta.
    end_offsets = consumer.end_offsets(topic_partitions)


    # === POSIZIONA IL CONSUMER ALL'INIZIO DELLA PARTIZIONE ===
    # In questo modo leggiamo TUTTI i messaggi dalla partizione, fino al limite stabilito sopra.
    for tp in topic_partitions:
        consumer.seek(tp, 0)

    records = []

    # === LEGGI I MESSAGGI FINO AGLI OFFSET FINALI ===
    for tp in topic_partitions:
        end_offset = end_offsets[tp]
        while consumer.position(tp) < end_offset:
            msg_pack = consumer.poll(timeout_ms=100)
            for _, msgs in msg_pack.items():
                for message in msgs:
                    line = message.value.strip()
                    if not line:
                        continue
                    row = line.split(',')
                    if len(row) == 0:
                        continue
                    records.append(row)

    if not records:
        print("[INFO] Nessun nuovo messaggio da scrivere.")
        return

    # === CREA IL NOME DEL FILE CON TIMESTAMP ===
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    topic_name = topic.replace("-results", "")
    filename = f"{topic_name}_{timestamp}.csv"
    output_file = os.path.join(output_dir, filename)

    print(f"[INFO] Scrittura su: {output_file}")
    with open(output_file, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Aggiunta header in base al topic che stiamo considerando (ovvero in base alla query)
        if "query1" in topic:
            header = ["seq_id", "print_id", "tile_id", "saturated"]
            writer.writerow(header)
        elif "query2" in topic:
            header = ["seq_id", "print_id", "tile_id", "P1", "dP1", "P2", "dP2", "P3", "dP3", "P4", "dP4", "P5", "dP5"]
            writer.writerow(header)
        elif "query3" in topic: 
            header = ["seq_id", "print_id", "tile_id", "saturated", "centroids"]
            writer.writerow(header)
        else:
            pass

        for row in records:
            writer.writerow(row)

    print(f"[INFO] Completato. Salvati {len(records)} record nel file '{filename}'.")

if __name__ == "__main__":
    main()
