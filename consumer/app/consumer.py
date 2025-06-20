import argparse
import csv
import os
import json
import time
from datetime import datetime

import requests
from kafka import KafkaConsumer, TopicPartition

from convert_to_json import csv_cluster_to_jsonl


class KafkaResultConsumer:
    """Gestisce il consumo di messaggi Kafka, il salvataggio su file e l'invio dei risultati all'API."""
    
    def __init__(self, broker, topic, bench_topic, api_url, output_dir="/results"):
        self.broker = broker
        self.topic = topic 
        self.bench_topic = bench_topic
        self.api_url = api_url
        self.output_dir = output_dir
        self.session = requests.Session()
        self.batch_processed = set()
        
        # Creiamo la directory di output se non esiste
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Determiniamo il tipo di query dal nome del topic
        self.query_type = self._get_query_type()
        
    def _get_query_type(self):
        if "query1" in self.topic:
            return "query1"
        elif "query2" in self.topic:
            return "query2"
        elif "query3" in self.topic:
            return "query3"
        else:
            raise ValueError(f"Topic non riconosciuto: {self.topic}")
    
    def wait_for_topics(self):
        self._wait_for_topic(self.bench_topic)
        self._wait_for_topic(self.topic)
    
    def _wait_for_topic(self, topic):
        """Attende che un singolo topic sia disponibile."""
        consumer = KafkaConsumer(bootstrap_servers=self.broker)
        print(f"[INFO] In attesa del topic '{topic}'...")
        
        while True:
            topics = consumer.topics()
            if topic in topics:
                print(f"[INFO] Topic '{topic}' trovato.")
                consumer.close()
                return
            time.sleep(2)
    
    def retrieve_bench_id(self):
        """Recupera l'ultimo messaggio dal topic bench_topic per ottenere il bench_id."""
        bench_consumer = KafkaConsumer(
            bootstrap_servers=[self.broker],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: m.decode('utf-8')
        )
        
        bench_id = None
        bench_id_partition = bench_consumer.partitions_for_topic(self.bench_topic)
        
        if bench_id_partition:
            topic_partitions = [TopicPartition(self.bench_topic, p) for p in bench_id_partition]
            bench_consumer.assign(topic_partitions)
            
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
            print(f"[ERRORE] Impossibile leggere il bench_id dal topic '{self.bench_topic}'.")
            return None
        
        print(f"[INFO] Letto bench_id dal topic '{self.bench_topic}': {bench_id}")
        return bench_id
    
    def setup_output_files(self):
        """Configura i file di output per CSV e JSONL."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        topic_name = self.topic.replace("-results", "")
        filename = f"{topic_name}_{timestamp}.csv"
        
        self.output_file = os.path.join(self.output_dir, filename)
        self.jsonl_file = self.output_file.replace(".csv", ".jsonl")
        
        print(f"[INFO] Scrittura su: {self.output_file}")
        
        # Crea e inizializza il file CSV con l'header appropriato
        with open(self.output_file, mode='w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            if self.query_type == "query1":
                header = ["seq_id", "print_id", "tile_id", "saturated"]
            elif self.query_type == "query2":
                header = ["seq_id", "print_id", "tile_id", "P1", "dP1", "P2", "dP2", "P3", "dP3", "P4", "dP4", "P5", "dP5"]
            elif self.query_type == "query3":
                header = ["seq_id", "print_id", "tile_id", "saturated", "centroids"]
            
            writer.writerow(header)
        
        return self.output_file, self.jsonl_file
    
    def create_consumer(self):
        """Crea e configura il consumer Kafka per il topic di risultati."""
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.broker],
            auto_offset_reset='earliest',
            group_id=f"result-consumer-{datetime.now().timestamp()}",
            value_deserializer=lambda m: m.decode('utf-8')
        )
    
    def process_results(self, bench_id):
        """Consuma messaggi dal topic dei risultati, li salva e li invia all'API per Query 3."""
        consumer = self.create_consumer()
        all_records = []
        all_data_processed = False
        last_message_time = datetime.now()
        
        print(f"[INFO] Inizio elaborazione continua. Output su: {self.output_file}")
        
        try:
            while not all_data_processed:
                message_batch = consumer.poll(timeout_ms=200)
                
                if message_batch:
                    last_message_time = datetime.now()
                    
                    for _, messages in message_batch.items():   # Nel batch di messaggi
                        for message in messages:                # processiamo ogni messaggio
                            record = message.value.strip()
                            if not record:
                                continue
                            
                            row = record.split(",")
                            if len(row) < 3:  # Minimo seq_id, print_id, tile_id
                                continue
                            
                            batch_id = row[0]
                            all_records.append(row)
                            
                            # Salviamo il record nel file CSV
                            with open(self.output_file, mode='a', newline='') as csvfile:
                                writer = csv.writer(csvfile)
                                writer.writerow(row)
                            
                            # Nel caso di query 3 dobbiamo poi inviare i risultati al challenger
                            if self.query_type == "query3" and batch_id not in self.batch_processed:
                                self._process_query3_result(batch_id, bench_id)
                else:
                    # Se non ci sono messaggi per 10 secondi, termina
                    if (datetime.now() - last_message_time).total_seconds() > 30:
                        print("[INFO] Nessun messaggio ricevuto per 30 secondi, elaborazione completata")
                        all_data_processed = True
                
                time.sleep(0.01)  # Pausa breve per non sovraccaricare la CPU
                
        except KeyboardInterrupt:
            print("[INFO] Elaborazione interrotta dall'utente")
        except Exception as e:
            print(f"[ERRORE] Si è verificato un errore: {e}")
        finally:
            consumer.close()
            print(f"[INFO] Totale record elaborati: {len(all_records)}")
            
            # Chiusura benchmark per query3 in modo da ottenere i risultati finali
            if self.query_type == "query3":
                self._end_benchmark(bench_id)
    
    def _process_query3_result(self, batch_id, bench_id):
        """Processa e invia un risultato della query3 all'API di benchmark."""
        try:
            # Conversione CSV in JSONL
            # TODO: vedere se dobbiamo usare JSON o il formato binario come accade nel client_ref
            csv_cluster_to_jsonl(self.output_file, self.jsonl_file)
            
            # Trova la riga corrispondente al batch_id
            batch_entry = None
            with open(self.jsonl_file, "r", encoding="utf-8") as f:
                for line in f:
                    entry = json.loads(line)
                    if str(entry["batch_id"]) == str(batch_id):
                        batch_entry = entry
                        break
            
            if batch_entry:
                # Invia il risultato all'API
                response = self.session.post(
                    f"{self.api_url}/api/result/0/{bench_id}/{batch_id}",
                    json=batch_entry
                )
                print(f"[INFO] Risultato per batch {batch_id} inviato: {response.status_code}")
                self.batch_processed.add(batch_id)
        except Exception as e:
            print(f"[ERRORE] Elaborazione batch {batch_id} fallita: {str(e)}")
    
    def _end_benchmark(self, bench_id):
        print("[INFO] Chiusura del benchmark...")
        try:
            end_resp = self.session.post(f"{self.api_url}/api/end/{bench_id}")
            print(f"[INFO] Benchmark {bench_id} terminato. Risposta: {end_resp.text}")
        except Exception as e:
            print(f"[ERRORE] Errore durante la chiusura del benchmark: {e}")


def parse_arguments():
    parser = argparse.ArgumentParser(description="Kafka Consumer per salvare i messaggi in CSV")
    parser.add_argument("--topic", required=True, help="Nome del topic Kafka da consumare")
    parser.add_argument("--bench_topic", required=True, help="Topic che contiene l'ID del bench")
    parser.add_argument("--broker", default="kafka:9092", help="Indirizzo del broker Kafka (default: kafka:9092)")
    parser.add_argument("--api_url", default="http://gc-challenger:8866", help="URL del local challenger")
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    # Inizializzazione del consumer
    consumer = KafkaResultConsumer(
        broker=args.broker,
        topic=args.topic,
        bench_topic=args.bench_topic,
        api_url=args.api_url
    )
    
    consumer.wait_for_topics()
    
    # Recuper del bench_id (ci servirà per inviare i risultati e chiudere il benchmark)
    bench_id = consumer.retrieve_bench_id()
    if not bench_id:
        return

    consumer.setup_output_files()
    
    # Elaborazione dei risultati
    consumer.process_results(bench_id)


if __name__ == "__main__":
    main()