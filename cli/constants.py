import os

CONFIG = {
    "paths": {
        "flink": {
            "jar_name": "flink-1.0-SNAPSHOT.jar",
            "local_jar_path": os.path.join("flink", "target", "flink-1.0-SNAPSHOT.jar"),
            "container_jar_path": "/opt/flink/jobs/flink-1.0-SNAPSHOT.jar",
            "job_class": "it.flink.StreamingJob"
        },
        "kafka_streams": {
            "jar_name": "kafkastreams-1.0-SNAPSHOT-jar-with-dependencies.jar",
            "local_jar_path": os.path.join("kafkastreams", "target", "kafkastreams-1.0-SNAPSHOT-jar-with-dependencies.jar")
        }
    },
    "services": {
        "base": ["kafka", "gc-challenger", "grafana", "prometheus"],
        "producer_consumer": ["producer", "consumer"]
    },
    "consumer": {
        "script": "/app/consumer.py",
        "script_short": "/app/consumer_short.py"
    },
    "topics": {
        "all": ["query3-results", "query2-results", "query1-results"],
        "short": ["query2-results", "query1-results"],
        "bench": "gc-bench"
    },
    "api": {
        "url": "http://gc-challenger:8866"
    },
    "wait_times": {
        "base_services": 10,
        "processing_services": 15
    }
}


# I codici seguono lo standard ANSI escape sequences, dove \033[ è il carattere di escape che indica l'inizio di una sequenza ANSI.
# È importante usare ENDC alla fine per ripristinare lo stile normale, altrimenti gli effetti continueranno a essere applicati.
class Colors:
    ENDC = '\033[0m'            # Testo per terminare tutti gli effetti di formattazione
    BOLD = '\033[1m'            # Testo in grassetto
    UNDERLINE = '\033[4m'       # Testo sottolineato
    
    RED = '\033[91m'            # Testo rosso
    GREEN = '\033[92m'          # Testo verde
    YELLOW = '\033[93m'         # Testo giall
    BLUE = '\033[94m'           # Testo blu
    HEADER = '\033[95m'         # Testo viola/magenta per l'intestazione
    CYAN = '\033[96m'           # Testo ciano