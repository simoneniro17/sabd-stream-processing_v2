import argparse
import os
import subprocess
import time
import sys
from typing import List

# Costanti
FLINK_JAR_NAME = "flink-1.0-SNAPSHOT.jar"
LOCAL_JAR_PATH = os.path.join("flink", "target", FLINK_JAR_NAME)
CONTAINER_JAR_PATH = "/opt/flink/jobs/flink-1.0-SNAPSHOT.jar"
FLINK_JOB_CLASS = "it.flink.StreamingJob"

CONSUMER_SCRIPT = "/app/consumer.py"
KAFKA_TOPICS = ["query3-results", "query1-results", "query2-results"]
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

def start_services() -> bool:
    """Avvia i servizi con docker-compose in modo sequenziale"""
    print("Avvio dei servizi in modo sequenziale...")
    
    # 1. Avvia kafka
    print("1. Avvio del servizio Kafka e del LOCAL-CHALLENGER...")
    success = run_command(["docker", "compose", "up", "-d", "kafka", "gc-challenger", "prometheus"])
    if not success:
        print("Errore nell'avviare il servizio Kafka o LOCAL-CHALLENGER")
        return False
    
    print("Attesa di 10 secondi per l'inizializzazione di Kafka e LOCAL-CHALLENGER...")
    time.sleep(10)
    
    # 2. Avvia flink-client
    print("2. Avvio del servizio Flink Client...")
    success = run_command(["docker", "compose", "up", "-d", "flink-client"])
    if not success:
        print("Errore nell'avviare il servizio Flink Client")
        return False
    
    print("Attesa di 15 secondi per l'inizializzazione di Flink Client...")
    time.sleep(15)
    
    # 3. Avvia producer e consumer
    print("3. Avvio dei servizi Producer e Consumer...")
    success = run_command(["docker", "compose", "up", "-d", "producer", "consumer"])
    if not success:
        print("Errore nell'avviare i servizi Producer e Consumer")
        return False
    
    print("Tutti i servizi sono stati avviati con successo")
    return True

def build_flink_jar() -> bool:
    """Compila il JAR di Flink"""
    print("Compilazione del JAR Flink...")
    
    # Ottieni il percorso assoluto della directory flink
    flink_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flink")
    
    # Controllo se siamo su Windows
    if sys.platform.startswith('win'):
        # Su Windows cerchiamo mvn.cmd che è lo script batch per Maven
        maven_cmd = "mvn.cmd"
    else:
        maven_cmd = "mvn"
        
    return run_command([maven_cmd, "clean", "package"], cwd=flink_dir)

def submit_flink_job() -> bool:
    """Invia il job Flink al JobManager usando il client-flink"""
    print("Invio del job Flink tramite client-flink...")
    
    # Verifichiamo che il JAR esista localmente prima di tentare di inviare il job
    if not os.path.exists(LOCAL_JAR_PATH):
        print(f"Errore: Il file JAR {LOCAL_JAR_PATH} non esiste. Esegui prima la build.")
        return False
        
    return run_command([
        "docker", "exec", "flink-client", 
        "flink", "run", 
        "--jobmanager", "jobmanager:8081",
        "-c", FLINK_JOB_CLASS, 
        CONTAINER_JAR_PATH
    ])

def read_kafka_topics(topics=None) -> bool:
    """Legge i dati dai topic Kafka specificati o da tutti"""
    topics = topics or KAFKA_TOPICS
    print(f"Lettura dei dati dai topic Kafka: {', '.join(topics)}")
    
    success = True
    for topic in topics:
        command = [
            "docker", "exec", "consumer",
            "python", CONSUMER_SCRIPT,
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
                       help="Compila il JAR di Flink")
    parser.add_argument("--deploy", action="store_true", 
                       help="Invia il job Flink al JobManager")
    parser.add_argument("--read", action="store_true", 
                       help="Legge i dati dai topic Kafka")
    parser.add_argument("--topic", nargs='+', 
                       help="Specifica i topic da leggere (usato con --read)")
    
    args = parser.parse_args()

    if args.all or args.build:
        down_services()
        build_flink_jar()

    if args.all or args.start:
        start_services()

    if args.all or args.read:
        read_kafka_topics(args.topic)
    
    print("Operazioni completate")

if __name__ == "__main__":
    main()