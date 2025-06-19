import argparse
import os
import csv
from datetime import datetime
import requests; 
from kafka import KafkaConsumer, TopicPartition
import json
from convert_to_json import csv_cluster_to_jsonl


def main():
    # === PARSING DEGLI ARGOMENTI DA RIGA DI COMANDO ===
    parser = argparse.ArgumentParser(description="Kafka Consumer per salvare i messaggi in CSV")
    parser.add_argument("--topic", required=True, help="Nome del topic Kafka da consumare")
    parser.add_argument("--bench_topic", required=True, help="Topic che contiene l'ID del bench")
    parser.add_argument("--broker", default="kafka:9092", help="Indirizzo del broker Kafka (default: kafka:9092)")
    parser.add_argument("--api_url", help="URL del local challenger")
    args = parser.parse_args()
    topic = args.topic
    broker = args.broker
    api_url = args.api_url
    bench_topic = args.bench_topic

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


    # === INVIA I RISULTATI AL LOCAL CHALLNGER ===
    # Se siamo in query3 dobbiamo inviare i risultati al LOCAL CHALLNGER
    if "query3" in topic:

        # Trasformiamo il csv di out in jsonl (linee di file json)
        jsonl_file = None

        jsonl_file = output_file.replace(".csv", ".jsonl")
        num_jsonl = csv_cluster_to_jsonl(output_file, jsonl_file)
        print(f"[INFO] File JSONL creato: {jsonl_file} con {num_jsonl} record.")

        # Leggi il bench_id da kafka
        bench_id_partition = consumer.partitions_for_topic(bench_topic)
        if not bench_id_partition:
            raise RuntimeError(f"Nessuna partizione trovata per il topic '{bench_topic}'.")

        topic_partitions = [TopicPartition(bench_topic, p) for p in bench_id_partition]
        consumer.assign(topic_partitions)

        # Vai all'ultimo offset - 1 per leggere l'ultimo messaggio
        for tp in topic_partitions:
            end_offset = consumer.end_offsets([tp])[tp]
            if end_offset == 0:
                raise RuntimeError(f"Nessun messaggio presente nel topic '{bench_topic}'.")
            consumer.seek(tp, end_offset - 1)

        bench_id = None
        msg_pack = consumer.poll(timeout_ms=500)
        for _, msgs in msg_pack.items():
            for message in msgs:
                bench_id = message.value.strip()
                break

        if not bench_id:
            raise RuntimeError("Impossibile leggere il bench_id dal topic Kafka.")
        
        print(f"[INFO] Letto bench_id dal topic '{bench_topic}': {bench_id}")

        # Invia il  messaggio al local challenger
        session = requests.Session()
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    payload = json.loads(line)
                    batch_id = payload["batch_id"]
                    response = session.post(f"{api_url}/api/result/0/{bench_id}/{batch_id}", json={
                        "batch_id": batch_id,
                        "status": "processed"
                    })
                    response.raise_for_status()
                    print(f"[INFO] Batch {batch_id} inviato con successo.")
                except Exception as e:
                    print(f"[ERRORE] Invio fallito per batch: {e}")

        try:
            end_resp = session.post(f"{api_url}/api/end/{bench_id}")
            end_resp.raise_for_status()
            result = end_resp.text
            print(f"[INFO] Benchmark {bench_id} terminato con successo. Risultati: {result}")
        except Exception as e:
            print(f"[ERRORE] Chiusura benchmark fallita: {e}") 


if __name__ == "__main__":
    main()
