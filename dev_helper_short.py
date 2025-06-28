import argparse
import os
import subprocess
import time
import sys
from typing import List

# Costanti
FLINK_JAR_NAME = "flink-1.0-SNAPSHOT.jar"
LOCAL_FLINK_JAR_PATH = os.path.join("flink", "target", FLINK_JAR_NAME)
CONTAINER_FLINK_JAR_PATH = "/opt/flink/jobs/flink-1.0-SNAPSHOT.jar"
FLINK_JOB_CLASS = "it.flink.StreamingJob"

KAFKA_STREAMS_JAR_NAME = "kafkastreams-1.0-SNAPSHOT-jar-with-dependencies.jar"
LOCAL_KS_JAR_PATH = os.path.join("kafkastreams", "target", KAFKA_STREAMS_JAR_NAME)

CONSUMER_SCRIPT_SHORT = "/app/consumer_short.py"
ALL_TOPICS = ["query2-results","query1-results"]

BENCH_TOPIC = "gc-bench"
API_URL = 'http://gc-challenger:8866'
SERVICE_START_WAIT = 15

def run_command(command: List[str], cwd=None) -> bool:
    """Esegue un comando e restituisce True se è andato a buon fine"""
    print(f"Esecuzione: {' '.join(command)}")
    try:
        result = subprocess.run(command, check=True, cwd=cwd)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Errore nell'esecuzione del comando: {e}")
        return False
    except Exception as e:
        print(f"Errore: {str(e)}")
        return False
    
def down_services() -> bool:
    print("DISTRUGGENDO i container attivi...")
    success = run_command(["docker", "compose", "down", "-v"])
    if not success:
        print("Errore nella DISTRUZIONE dei container")
        return False
    return True

def start_services(use_kafka_streams=False) -> bool:
    """Avvia i servizi con docker-compose in modo sequenziale"""
    print("Avvio dei servizi in modo sequenziale...")
    
    # 1. Avvia servizi di base: kafka, gc-challenger, prometheus e grafana
    print("1. Avvio dei servizi base: Kafka, LOCAL-CHALLENGER, Prometheus e Grafana...")
    success = run_command(["docker", "compose", "up", "-d", "kafka", "gc-challenger"])
    if not success:
        print("Errore nell'avviare i servizi base")
        return False
    
    print(f"Attesa di 10 secondi per l'inizializzazione dei servizi base...")
    time.sleep(10)
    
    # 2. Avvia il servizio di elaborazione dati (Flink o Kafka Streams)
    if use_kafka_streams:
        print("2. Avvio del servizio Kafka Streams...")
        success = run_command(["docker", "compose", "up", "-d", "kafka-streams"])
    else:
        print("2. Avvio del servizio Flink Client...")
        success = run_command(["docker", "compose", "up", "-d", "flink-client"])
    
    if not success:
        print(f"Errore nell'avviare il servizio di elaborazione dati")
        return False
    
    print(f"Attesa di 15 secondi per l'inizializzazione del servizio di elaborazione dati...")
    time.sleep(15)
    
    # 3. Avvia producer e consumer
    print("3. Avvio dei servizi Producer e Consumer...")
    success = run_command(["docker", "compose", "up", "-d", "producer", "consumer"])
    if not success:
        print("Errore nell'avviare i servizi Producer e Consumer")
        return False
    
    print("Tutti i servizi sono stati avviati con successo")
    return True

def build_jar(use_kafka_streams=False) -> bool:
    """Compila il JAR di Flink o Kafka Streams"""
    if use_kafka_streams:
        project_dir = "kafkastreams"
        print("Compilazione del JAR Kafka Streams...")
    else:
        project_dir = "flink"
        print("Compilazione del JAR Flink...")
    
    # Ottieni il percorso assoluto della directory del progetto
    project_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), project_dir)
    
    # Controllo se siamo su Windows
    if sys.platform.startswith('win'):
        # Su Windows cerchiamo mvn.cmd che è lo script batch per Maven
        maven_cmd = "mvn.cmd"
    else:
        maven_cmd = "mvn"
        
    return run_command([maven_cmd, "clean", "package"], cwd=project_path)

def submit_flink_job() -> bool:
    """Invia il job Flink al JobManager usando il client-flink"""
    print("Invio del job Flink tramite client-flink...")
    
    # Verifichiamo che il JAR esista localmente prima di tentare di inviare il job
    if not os.path.exists(LOCAL_FLINK_JAR_PATH):
        print(f"Errore: Il file JAR {LOCAL_FLINK_JAR_PATH} non esiste. Esegui prima la build.")
        return False
        
    return run_command([
        "docker", "exec", "flink-client", 
        "flink", "run", 
        "--jobmanager", "jobmanager:8081",
        "-c", FLINK_JOB_CLASS, 
        CONTAINER_FLINK_JAR_PATH
    ])

def read_kafka_topics(use_kafka_streams=False, topics=None) -> bool:
    """Legge i dati dai topic Kafka specificati o da tutti"""
    if topics:
        topics_to_read = topics
    else:
        topics_to_read = ALL_TOPICS
        
    print(f"Lettura dei dati dai topic Kafka: {', '.join(topics_to_read)}")
    
    success = True
    for topic in topics_to_read:
        command = [
            "docker", "exec", "consumer",
            "python", CONSUMER_SCRIPT_SHORT,
            "--topic", topic,
            "--bench_topic", BENCH_TOPIC,
            "--api_url", API_URL
        ]
        if not run_command(command):
            success = False
    
    return success

def main():
    parser = argparse.ArgumentParser(
        description="Automazione del flusso di sviluppo per il progetto SABD Stream Processing"
    )
    
    parser.add_argument("--all", action="store_true", 
                       help="Esegue l'intero flusso (start, build, deploy, read)")
    parser.add_argument("--start", action="store_true", 
                       help="Avvia i servizi con docker-compose")
    parser.add_argument("--build", action="store_true", 
                       help="Compila il JAR del progetto")
    parser.add_argument("--deploy", action="store_true", 
                       help="Invia il job Flink al JobManager (solo per Flink)")
    parser.add_argument("--read", action="store_true", 
                       help="Legge i dati dai topic Kafka")
    parser.add_argument("--topic", nargs='+', 
                       help="Specifica i topic da leggere (usato con --read)")
    parser.add_argument("--k", action="store_true",
                       help="Usa Kafka Streams invece di Flink")
    
    args = parser.parse_args()
    
    use_kafka_streams = args.k

    if args.all or args.build:
        down_services()
        build_jar(use_kafka_streams)

    if args.all or args.start:
        start_services(use_kafka_streams)

    if args.all or args.read:
        read_kafka_topics(use_kafka_streams, args.topic)
    
    print("Operazioni completate")

if __name__ == "__main__":
    main()