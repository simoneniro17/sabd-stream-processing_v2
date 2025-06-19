import requests
import time
import sys
import argparse

from kafka import KafkaProducer

# === CONFIG ===
KAFKA_BROKER = 'kafka:9092'
BATCHES_TOPIC = 'gc-batches'
BENCH_TOPIC = 'gc-bench'
API_URL = 'http://gc-challenger:8866'
MAX_RETRIES = 10
WAIT_TIME = 3   # tempo di attesa (s) tra i tentativi di connessione


def main():
    parser = argparse.ArgumentParser(description="Producer per la scrittura dei dati del local challenger su Kafka")
    parser.add_argument("--limit", type=int, default=None, help="Numero massimo di batch da produrre")
    args = parser.parse_args()

    max_batches = args.limit

    # === SETUP di Kafka ===
    producer = None
    session = requests.Session()
    
    # Tentativo di connessione al broker Kafka
    for attempt in range(MAX_RETRIES):
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
            print(f"Connesso a Kafka su {KAFKA_BROKER}")
            break
        except Exception as e:
            print(f"Errore di connessione a Kafka: {e}. Tentativo {attempt + 1}/{MAX_RETRIES}")
            time.sleep(WAIT_TIME)
    else:
        print("Impossibile connettersi a Kafka.")
        sys.exit(1)

    # Tentativo di connessione al challenger
    for attempt in range(MAX_RETRIES):
        try:
            # Verifichiamo la connessione al Challenger provando a fare una richiesta alla sua API di dash
            # visto che il GC non mette a disposizione un endpoint di healthcheck
            test_response = session.get(f"{API_URL}/dash")
            test_response.raise_for_status()
            print(f"Connesso al Challenger su {API_URL}")
            break
        except requests.RequestException as e:
            print(f"Errore di connessione al challenger: {e}. Tentativo {attempt + 1}/{MAX_RETRIES}")
            time.sleep(WAIT_TIME)
    else:
        print("Impossibile connettersi al Challenger.")
        sys.exit(1)

    # === CREAZIONE E AVVIO DELLA SESSIONE DI BENCHMARK ===
    print("Creazione della sessione di benchmark...")
    create_params = {
        "apitoken": "polimi-deib",
        "name": "kafka-pipeline",
        "test": True
    }
    
    # Se il limite è specificato, lo aggiungiamo ai parametri (così evitiamo di dover per forza inviare tutti i batch)
    if max_batches is not None:
        create_params["max_batches"] = max_batches

    create_resp = session.post(f"{API_URL}/api/create", json=create_params)
    create_resp.raise_for_status()
    bench_id = create_resp.json()
    print(f"Benchmark ID: {bench_id}")

    print("Avvio benchmark...")
    start_resp = session.post(f"{API_URL}/api/start/{bench_id}")

    # Produzione del messaggio con il Benchmark ID su Kafka
    producer.send(BENCH_TOPIC, value=bench_id.encode('utf-8'))
    producer.flush()
    print(f"Prodotto Benchmark ID su Kafka.")

    # === PRODUZIONE DEI BATCH ===
    i = 0
    try:
        while True:
            # Controllo del limite di batch
            if max_batches is not None and i >= max_batches:
                print(f"Raggiunto il limite di {max_batches} batch.")
                break
            
            print(f"Richiesta del batch {i}...")
            batch_resp = session.get(f"{API_URL}/api/next_batch/{bench_id}")
            if batch_resp.status_code == 404:
                print("Nessun altro batch disponibile.")
                break
            batch_resp.raise_for_status()

            # Produzione del messaggio con i batch su Kafka
            producer.send(BATCHES_TOPIC, value=batch_resp.content)
            producer.flush()    # Ci assicuriamo che il messaggio sia stato inviato prima di continuare
            print(f"Prodotto batch {i} su Kafka.")

            # Risultato fittizio al challenger, soltanto per evitare che il producer crashi quando finisce
            result_resp = session.post(
                f"{API_URL}/api/result/0/{bench_id}/{i}",
                json={"batch_id": i, "status": "processed"}
            )

            i += 1
    except KeyboardInterrupt:
        print("Interruzione da tastiera. Terminazione della produzione.")
    except Exception as e:
        print(f"Errore durante la produzione: {e}")
    finally:
        # === FINE DELLA SESSIONE DI BENCHMARK ===
        end_resp = session.post(f"{API_URL}/api/end/{bench_id}")
        end_resp.raise_for_status()
        print("Sessione di benchmark terminata.")

        # Chiusura del producer
        if producer:
            producer.close()


if __name__ == "__main__":
    main()