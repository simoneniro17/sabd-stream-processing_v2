import requests
import time
import sys
import argparse
import logging

from kafka import KafkaProducer

# === CONFIG ===
KAFKA_BROKER = 'kafka:9092'
BATCHES_TOPIC = 'gc-batches'
BENCH_TOPIC = 'gc-bench'
API_URL = 'http://gc-challenger:8866'
MAX_RETRIES = 10
WAIT_TIME = 3       # tempo di attesa (s) tra i tentativi di connessione

# Configurazione del logging
logging.basicConfig(
    level=logging.INFO,
    format = '%(asctime)s - %(levelname)s - %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    handlers = [logging.StreamHandler(sys.stdout)]
)
for name in sorted(logging.root.manager.loggerDict):
    logging.getLogger(name).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

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
            logger.info(f"Connesso a Kafka su {KAFKA_BROKER}")
            break
        except Exception as e:
            logger.warning(f"Errore di connessione a Kafka: {e}. Tentativo {attempt + 1}/{MAX_RETRIES}")
            time.sleep(WAIT_TIME)
    else:
        logger.error("Impossibile connettersi a Kafka.")
        sys.exit(1)

    # Tentativo di connessione al challenger
    for attempt in range(MAX_RETRIES):
        try:
            # Verifichiamo la connessione al Challenger provando a fare una richiesta alla sua API di dash
            # visto che il GC non mette a disposizione un endpoint di healthcheck
            test_response = session.get(f"{API_URL}/dash")
            test_response.raise_for_status()
            logger.info(f"Connesso al Challenger su {API_URL}")
            break
        except requests.RequestException as e:
            logger.warning(f"Errore di connessione al challenger: {e}. Tentativo {attempt + 1}/{MAX_RETRIES}")
            time.sleep(WAIT_TIME)
    else:
        logger.error("Impossibile connettersi al Challenger.")
        sys.exit(1)

    # === CREAZIONE E AVVIO DELLA SESSIONE DI BENCHMARK ===
    logger.info("Creazione della sessione di benchmark...")
    create_params = {
        "apitoken": "polimi-deib",
        "name": "flink",
        "test": True
    }
    
    # Se il limite è specificato, lo aggiungiamo ai parametri (così evitiamo di dover per forza inviare tutti i batch)
    if max_batches is not None:
        create_params["max_batches"] = max_batches

    create_resp = session.post(f"{API_URL}/api/create", json=create_params)
    create_resp.raise_for_status()
    bench_id = create_resp.json()
    logger.info(f"Benchmark ID: {bench_id}")

    logger.info("Avvio benchmark...")
    session.post(f"{API_URL}/api/start/{bench_id}")

    # Scriviamo il Benchmark ID su Kafka affinché il consumer possa leggerlo e chiudere lui la connessione
    producer.send(BENCH_TOPIC, value=bench_id.encode('utf-8'))
    producer.flush()
    logger.info(f"Scritto Benchmark ID su Kafka.")

    # === PRODUZIONE DEI BATCH ===
    i = 0
    try:
        while True:
            # Controllo del limite di batch
            if max_batches is not None and i >= max_batches:
                logger.info(f"Raggiunto il limite di {max_batches} batch.")
                break
            
            if i % 16 == 0:
                logger.info(f"Richiesta del batch {i}...")
            batch_resp = session.get(f"{API_URL}/api/next_batch/{bench_id}")
            if batch_resp.status_code == 404:
                logger.info("Nessun altro batch disponibile.")
                break
            batch_resp.raise_for_status()

            # Produzione del messaggio con i batch su Kafka
            producer.send(BATCHES_TOPIC, value=batch_resp.content)
            producer.flush()    # Ci assicuriamo che il messaggio sia stato inviato prima di continuare
            if i % 16 == 0:
                logger.info(f"Scritto batch {i} su Kafka.")

            i += 1
    except KeyboardInterrupt:
        logger.info("Interruzione da tastiera. Terminazione della produzione.")
    except Exception as e:
        logger.error(f"Errore durante la produzione: {e}")
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    main()