#!/usr/bin/env python3
"""
Script di automazione semplificato per il progetto SABD Stream Processing.
"""

import argparse
import os
import subprocess
import time
import webbrowser
import sys
from typing import List

# Costanti
FLINK_JAR_NAME = "flink-1.0-SNAPSHOT.jar"
LOCAL_JAR_PATH = os.path.join("flink", "target", FLINK_JAR_NAME)
# Il percorso al JAR nel container (attraverso il volume condiviso)
CONTAINER_JAR_PATH = "/opt/flink/jobs/flink-1.0-SNAPSHOT.jar"
FLINK_JOB_CLASS = "it.flink.StreamingJob"
CONSUMER_SCRIPT = "/app/consumer.py"
KAFKA_TOPICS = ["saturation-results-topic", "outlier-results-topic"]
SERVICE_START_WAIT = 20  # Secondi di attesa dopo l'avvio dei servizi

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

def start_services() -> bool:
    """Avvia i servizi con docker-compose"""
    print("Avvio dei servizi...")
    success = run_command(["docker", "compose", "up", "-d"])
    if success:
        print(f"Attesa di {SERVICE_START_WAIT} secondi per l'inizializzazione dei servizi...")
        time.sleep(SERVICE_START_WAIT)
    return success

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
            "--topic", topic
        ]
        if not run_command(command):
            success = False
    
    return success

def open_flink_ui() -> None:
    """Apre l'interfaccia web di Flink nel browser"""
    webbrowser.open("http://localhost:8081")
    print("Interfaccia web di Flink aperta nel browser")

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
    parser.add_argument("--ui", action="store_true", 
                       help="Apre l'interfaccia web di Flink")
    parser.add_argument("--topic", nargs='+', 
                       help="Specifica i topic da leggere (usato con --read)")
    
    args = parser.parse_args()
    
    if args.all or args.start:
        start_services()
    
    if args.all or args.build:
        build_flink_jar()
    
    if args.all or args.deploy:
        submit_flink_job()
    
    if args.ui:
        open_flink_ui()
        
    if args.all or args.read:
        # Se --all, attendi un po' per lasciare al job il tempo di produrre risultati
        if args.all:
            print("Attesa per la generazione dei risultati...")
            time.sleep(5)
        read_kafka_topics(args.topic)
    
    print("Operazioni completate")

if __name__ == "__main__":
    main()