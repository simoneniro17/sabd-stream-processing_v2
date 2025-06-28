import argparse
import os
import subprocess
import time
import sys
from typing import List

from cli.printer import *
from cli.constants import Colors, CONFIG

def run_command(command, cwd=None):
    """Esegue un comando e restituisce True se è andato a buon fine"""
    cmd_str = ' '.join(command)
    print_info(f"Esecuzione: {Colors.CYAN}{cmd_str}{Colors.ENDC}")
    
    try:
        result = subprocess.run(command, check=True, cwd=cwd)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print_error(f"Comando fallito: {cmd_str}")
        print_error(f"Dettagli errore: {e}")
        return False
    except Exception as e:
        print_error(f"Eccezione durante l'esecuzione: {str(e)}")
        return False

def down_services():
    print_phase("Arresto dei servizi")
    print_info("Arresto di tutti i container in esecuzione...")
    
    success = run_command(["docker", "compose", "down", "-v"])
    if not success:
        print_error("Impossibile arrestare i container")
        return False
    
    print_success("Tutti i servizi sono stati arrestati correttamente")
    return True

def start_services(use_kafka_streams=False):
    """Avvia i servizi con docker-compose in modo sequenziale per massimizzare le prestazioni"""
    print_phase("Avvio dei servizi...")
    
    # 1. Servizi di base
    print_info("FASE 1: Avvio dei servizi base")
    base_services = CONFIG["services"]["base"]
    print_info(f"Avvio di: {', '.join(base_services)}...")
    
    success = run_command(["docker", "compose", "up", "-d"] + base_services)
    if not success:
        print_error("Impossibile avviare i servizi base")
        return False
    
    wait_time = CONFIG["wait_times"]["base_services"]
    print_warning(f"Attesa di {wait_time} secondi per l'inizializzazione dei servizi base...")
    time.sleep(wait_time)
    
    # 2. Servizi di processamento
    print("\n")
    print_info("FASE 2: Avvio del servizio di processamento")
    if use_kafka_streams:
        print_info("Avvio del servizio Kafka Streams...")
        success = run_command(["docker", "compose", "up", "-d", "kafka-streams"])
        service_name = "Kafka Streams"
    else:
        print_info("Avvio del cluster Flink...")
        success = run_command(["docker", "compose", "up", "-d", "flink-client"])
        service_name = "Flink"
    
    if not success:
        print_error(f"Impossibile avviare {service_name}")
        return False
    
    wait_time = CONFIG["wait_times"]["processing_services"]
    print_warning(f"Attesa di {wait_time} secondi per l'inizializzazione di {service_name}...")
    time.sleep(wait_time)
    
    # 3. Producer e consumer
    print("\n")
    print_info("FASE 3: Avvio di producer e consumer")
    producer_consumer = CONFIG["services"]["producer_consumer"]
    print_info(f"Avvio di {', '.join(producer_consumer)}...")
    success = run_command(["docker", "compose", "up", "-d"] + producer_consumer)
    if not success:
        print_error("Impossibile avviare Producer e Consumer")
        return False
    
    print_success("Tutti i servizi sono stati avviati correttamente")
    return True

def build_jar(use_kafka_streams=False):
    """Compila il JAR di Flink o Kafka Streams"""
    engine_type = "Kafka Streams" if use_kafka_streams else "Flink"
    project_dir = "kafkastreams" if use_kafka_streams else "flink"
    
    print_phase(f"Compilazione del JAR {engine_type}...")
    
    # Percorso assoluto della directory del progetto
    project_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), project_dir)
    
    # Controllo se siamo su Windows
    maven_cmd = "mvn.cmd" if sys.platform.startswith('win') else "mvn"
    
    success = run_command([maven_cmd, "clean", "package"], cwd=project_path)
    
    if success:
        print_success(f"JAR di {engine_type} compilato con successo")
    else:
        print_error(f"Errore durante la compilazione del JAR {engine_type}")
    
    return success

def show_logs(service=None):
    """Mostra i log di un servizio specifico o di tutti i servizi"""
    if service:
        print_phase(f"Log del servizio {service.upper()}:")
        return run_command(["docker", "compose", "logs", service])
    else:
        print_phase("Log di tutti i servizi:")
        return run_command(["docker", "compose", "logs"])

def read_kafka_topics(use_kafka_streams=False, use_short=False):
    """Legge i dati dai topic Kafka specificati"""
    print_phase("Lettura dei risultati dai topic Kafka...")
    
    # Determina quali topic leggere
    if use_short or use_kafka_streams:
        print_warning("Verranno letti solo i topic di Query 1 e 2")
        topics_to_read = CONFIG["topics"]["short"]  # Anche con "--all" Kafka può comunque leggere solo 2 topic
    else:
        topics_to_read = CONFIG["topics"]["all"]
        
    print_info(f"Topic da leggere (in ordine): {', '.join(topics_to_read)}")
    
    # Script del consumer a seconda della modalità di esecuzione
    consumer_script = CONFIG["consumer"]["script_short"] if use_short else CONFIG["consumer"]["script"]
    
    success = True
    for topic in topics_to_read:
        print_info(f"Elaborazione del topic: {Colors.CYAN}{topic}{Colors.ENDC}")
        
        command = [
            "docker", "exec", "consumer",
            "python", consumer_script,
            "--topic", topic,
            "--bench_topic", CONFIG["topics"]["bench"],
            "--api_url", CONFIG["api"]["url"]
        ]
        
        if not run_command(command):
            print_error(f"Errore durante la lettura del topic {topic}")
            success = False
            if topic == topics_to_read[0]:
                print_error("Errore nel topic principale: il benchmark non verrà chiuso.")
        
        print("\n")
    
    if success:
        print_success("Tutti i topic sono stati elaborati con successo")
    
    return success

def main():
    print_header("SABD - Progetto Stream Processing")
    
    parser = argparse.ArgumentParser(description="SABD - Progetto Stream Processing")
    
    # Scelta del motore di processing
    parser.add_argument("--engine", choices=["flink", "kafka-streams"], default="flink",
                      help="Framework da utilizzare per il processamento (default: flink)")
    
    # Gruppo mutuamente esclusivo di comandi principali
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", 
                       help="Esegue il flusso completo (ordinato per massimizzare prestazioni)")
    group.add_argument("--logs", nargs='?', const="all", metavar="SERVICE",
                       help="Visualizza i log (di un servizio specifico o di tutti)")
    
    # Opzione aggiuntiva per la versione dell'analisi che si ferma a Q2
    parser.add_argument("--short", action="store_true",
                       help="Utilizza una versione ridotta dell'analisi che si ferma a Query 2")
    
    args = parser.parse_args()
    use_kafka_streams = args.engine == "kafka-streams"
    
    # Visualizzazione log se attivata
    if args.logs is not None:
        service = None if args.logs == "all" else args.logs
        show_logs(service)
        return
        
    # Flusso completo
    if args.all:
        engine_name = "Kafka Streams" if use_kafka_streams else "Flink"
        print_info(f"Avvio del flusso completo con {engine_name.upper()}")
        
        # 1. Buttiamo giù i container con tanto di volumi
        if not down_services():
            return
        
        # 2. Compiliamo il JAR
        if not build_jar(use_kafka_streams):
            return
        
        # 3. Avviamo i servizi in ordine
        if not start_services(use_kafka_streams):
            return
        
        # 4. Consumiamo i risultati
        read_kafka_topics(use_kafka_streams,args.short)
    
        print_success("Il flusso completo è stato eseguito con successo")

if __name__ == "__main__":
    main()