import argparse
import os
import csv
from datetime import datetime
import requests
from kafka import KafkaConsumer, TopicPartition
import json
from convert_to_json import csv_cluster_to_jsonl  # Nota: usa csv_cluster_to_jsonl, non csv_cluster_to_json
import time


def wait_for_topic(bootstrap_servers, topic):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    print(f"[INFO] In attesa del topic '{topic}'...")

    while True:
        topics = consumer.topics()
        if topic in topics:
            print(f"[INFO] Topic '{topic}' trovato.")
            consumer.close()
            return
        time.sleep(2)

def retrieve_bench_id(broker, bench_topic):
    bench_consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        auto_offset_reset='earliest',
        value_deserializer=lambda m: m.decode('utf-8')
    )

    bench_id = None
    bench_id_partition = bench_consumer.partitions_for_topic(bench_topic)
    if bench_id_partition:
        topic_partitions = [TopicPartition(bench_topic, p) for p in bench_id_partition]
        bench_consumer.assign(topic_partitions)
        
        # Cerca l'ultimo messaggio
        for tp in topic_partitions:
            end_offset = bench_consumer.end_offsets([tp])[tp]
            if end_offset > 0:
                bench_consumer.seek(tp, end_offset - 1)
                
                msg_pack = bench_consumer.poll(timeout_ms=1000)
                for _, msgs in msg_pack.items():
                    for message in msgs:
                        bench_id = message.value.strip()
                        break
    
    bench_consumer.close()

    if not bench_id:
        print(f"[ERRORE] Impossibile leggere il bench_id dal topic '{bench_topic}'.")
        return
    
    print(f"[INFO] Letto bench_id dal topic '{bench_topic}': {bench_id}")
    return bench_id


def main():
    # === PARSING DEGLI ARGOMENTI DA RIGA DI COMANDO ===
    parser = argparse.ArgumentParser(description="Kafka Consumer per salvare i messaggi in CSV")
    parser.add_argument("--topic", required=True, help="Nome del topic Kafka da consumare")
    parser.add_argument("--bench_topic", required=True, help="Topic che contiene l'ID del bench")
    parser.add_argument("--broker", default="kafka:9092", help="Indirizzo del broker Kafka (default: kafka:9092)")
    parser.add_argument("--api_url", default="http://gc-challenger:8866", help="URL del local challenger")
    args = parser.parse_args()
    topic = args.topic
    broker = args.broker
    api_url = args.api_url
    bench_topic = args.bench_topic

    # === CREA CARTELLA DI OUTPUT SE NON ESISTE ===
    output_dir = "/results"
    os.makedirs(output_dir, exist_ok=True)

    print(f"[INFO] Connessione a Kafka su '{broker}' e ascolto del topic '{topic}'...")

    wait_for_topic(broker, bench_topic)
    wait_for_topic(broker, topic)
    
    # === RECUPERA IL BENCH_ID DA KAFKA ===
    bench_id = retrieve_bench_id(broker,bench_topic)

    # === CONFIGURA IL CONSUMER KAFKA E LA SESSIONE HTTP ===
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        auto_offset_reset='earliest',
        group_id=f"result-consumer-{datetime.now().timestamp()}",
        value_deserializer=lambda m: m.decode('utf-8')
    )

    session = requests.Session()
    
    # === CREA IL NOME DEL FILE CON TIMESTAMP ===
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    topic_name = topic.replace("-results", "")
    filename = f"{topic_name}_{timestamp}.csv"
    output_file = os.path.join(output_dir, filename)
    jsonl_file = output_file.replace(".csv", ".jsonl")

    print(f"[INFO] Scrittura su: {output_file}")
    with open(output_file, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Aggiunta header in base al topic che stiamo considerando (ovvero in base alla query)
        if "query1" in topic:
            header = ["seq_id", "print_id", "tile_id", "saturated"]
        elif "query2" in topic:
            header = ["seq_id", "print_id", "tile_id", "P1", "dP1", "P2", "dP2", "P3", "dP3", "P4", "dP4", "P5", "dP5"]
        elif "query3" in topic: 
            header = ["seq_id", "print_id", "tile_id", "saturated", "centroids"]
        else:
            print(f"[ERRORE] Topic '{topic}' non riconosciuto. Specifica 'query1', 'query2' o 'query3'.")
            return
        writer.writerow(header)

    batch_processed = set()  # Per evitare duplicati
    print(f"[INFO] Inizio elaborazione continua. Output su: {output_file}")

    # Flag per capire se abbiamo finito il processing
    all_data_processed = False
    last_message_time = datetime.now()
    
    # Accumulatore per i messaggi
    all_records = []

    try:
        # Impostiamo un timeout di polling più lungo per attendere i messaggi
        while not all_data_processed:
            # Poll dei messaggi
            message_batch = consumer.poll(timeout_ms=200)
            
            if message_batch:
                # Resettiamo il timer perché abbiamo ricevuto messaggi
                last_message_time = datetime.now()
                
                for _, messages in message_batch.items():
                    for message in messages:
                        record = message.value.strip()
                        if not record:
                            continue

                        row = record.split(",")
                        if len(row) < 3:  # Minimo deve avere seq_id, print_id, tile_id
                            continue

                        batch_id = row[0]
                        all_records.append(row)
                        
                        # Scrivi ogni record nel file CSV
                        with open(output_file, mode='a', newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(row)

                        if "query3" in topic and batch_id not in batch_processed:
                            try:
                                # Converti CSV in JSONL (un JSON per riga)
                                csv_cluster_to_jsonl(output_file, jsonl_file)
                                
                                # Trova la riga corrispondente al batch_id
                                batch_entry = None
                                with open(jsonl_file, "r", encoding="utf-8") as f:
                                    for line in f:
                                        entry = json.loads(line)
                                        if str(entry["batch_id"]) == str(batch_id):
                                            batch_entry = entry
                                            break
                                
                                if batch_entry:
                                    # Invia il risultato
                                    response = session.post(
                                        f"{api_url}/api/result/0/{bench_id}/{batch_id}", 
                                        json=batch_entry
                                    )
                                    print(f"[INFO] Risultato per batch {batch_id} inviato: {response.status_code}")
                                    batch_processed.add(batch_id)
                            except Exception as e:
                                print(f"[ERRORE] Elaborazione batch {batch_id} fallita: {str(e)}")
            else:
                # Se sono passati più di 10 secondi senza messaggi, consideriamo il processing completato
                if (datetime.now() - last_message_time).total_seconds() > 10:
                    print("[INFO] Nessun messaggio ricevuto per 10 secondi, presumo che abbiamo finito")
                    all_data_processed = True
            
            # Breve pausa per non sovraccaricare la CPU
            time.sleep(0.1)
                
    except KeyboardInterrupt:
        print("[INFO] Elaborazione interrotta dall'utente")
    except Exception as e:
        print(f"[ERRORE] Si è verificato un errore: {e}")
    finally:
        consumer.close()
        
        print(f"[INFO] Totale record elaborati: {len(all_records)}")

        print("[INFO] Chiusura del benchmark...")
        if "query3" in topic:
            try:
                end_resp = session.post(f"{api_url}/api/end/{bench_id}")
                print(f"[INFO] Benchmark {bench_id} terminato. Risposta: {end_resp.text}")
            except Exception as e:
                print(f"[ERRORE] Errore durante la chiusura del benchmark: {e}")


if __name__ == "__main__":
    main()