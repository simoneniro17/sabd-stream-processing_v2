# Analisi Real-time di Difetti nella Produzione Manifatturiera - Progetto SABD (Stream Processing)

## Avvio e Utilizzo

### Requisiti
Per eseguire il progetto, è necessario avere installato:
- **Docker e Docker Compose**, per l'esecuzione dell'intero stack.
- **Java 11 e Maven**, per compilare sia il codice di Flink che quello di Kafka Streams.
- **Python 3.8+**, per eseguire il main (o il client_ref del challenger).

### Utilizzo
1. **Clonare il repository**
   ```bash
   git clone https://github.com/simoneniro17/sabd-stream-processing_v2
   cd sabd-stream-processing_v2
   ```

2. **Installare le dipendenze**
   ```bash
   pip install -r requirements.txt
   ```

3. **Copiare il GC25 Docker nella root del progetto**. Assicurarsi di avere una cartella `gc25-chall` contenente i file estratti dai due archivi `gc25-chall.tgz` e `gc25-chall-data.tgz`.

4. **Creare l'immagine Docker per il LOCAL_CHALLENGER**. Assicurarsi di avere Docker in esecuzione ed eseguire i seguenti comandi:
   ```bash
   cd gc25-chall
   docker image load -i gc25cdocker.tar
   cd ..
   ```

### Utilizzo del main
Tramite l'utilizzo dello script `main.py` è possibile automatizzare l'intero processo di esecuzione. Non trattandosi di un applicazione di batch processing, non c'è molto con cui interagire quindi non si tratta di una vera e propria CLI.

#### Esempi di utilizzo
1. **Esecuzione completa con Flink**:
   ```bash
   python main.py --all
   ```
2. **Esecuzione con Kafka Streams**:
   ```bash
   python main.py --all --engine kafka-streams
   ```
3. **Visualizzazione di tutte le opzioni disponibili**:
   ```bash
   python main.py --help
   ```

### Esecuzione da riga di comando
Ovviamente, è possibile eseguire l'applicazione anche senza utilizzare lo script `main.py`. In tal caso, occorre eseguire manualmente i passaggi fondamentali.

1. **Arrestare eventuali container attivi**:
   ```bash
   docker compose down -v
   ```

2. **Compilare il JAR appropriato**:
   ```bash
   # Per Flink
   cd flink
   mvn clean package
   cd ..
   ```
   ```bash
   # Per Kafka Stream
   cd kafkastreams
   mvn clean package
   cd ..
   ```

3. **Avviare i servizi (in sequenza se si vogliono le migliori prestazioni)**:
   ```bash
   # Servizi base
   docker compose up -d kafka gc-challenger grafana prometheus
   
   # Attendere circa 10 secondi per l'inizializzazione
   
   # Servizio di processing (Flink o Kafka Streams)
   docker compose up -d flink-client  # oppure: docker compose up -d kafka-streams
   
   # Attendere circa 15 secondi per l'inizializzazione
   
   # Producer e Consumer
   docker compose up -d producer consumer
   ```

4. **Leggere i risultati**:
   ```bash
   # Per l'esecuzione completa con Flink
   docker exec consumer python /app/consumer.py --topic query3-results --bench_topic gc-bench --api_url http://gc-challenger:8866
   docker exec consumer python /app/consumer.py --topic query2-results --bench_topic gc-bench --api_url http://gc-challenger:8866
   docker exec consumer python /app/consumer.py --topic query1-results --bench_topic gc-bench --api_url http://gc-challenger:8866

   # Per la modalità ridotta (fino a Q2) o con Kafka Streams:
   docker exec consumer python /app/consumer_short.py --topic query2-results --bench_topic gc-bench --api_url http://gc-challenger:8866
   docker exec consumer python /app/consumer_short.py --topic query1-results --bench_topic gc-bench --api_url http://gc-challenger:8866
   ```

## Architettura
```
├── Data Generation & Collection
│   ├── LOCAL-CHALLENGER (gc-challenger)
│   │   ├── Generazione eventi di produzione simulati
│   │   ├── Gestione benchmark e validazione risultati
│   │   └── API REST per interazione con producer/consumer
│   └── Producer
│       ├── Richiesta dati al LOCAL-CHALLENGER
│       └── Pubblicazione degli eventi sui topic Kafka
├── Message Broker (Apache Kafka)
│   ├── Topic di input (eventi di produzione)
│   ├── Topic di risultati delle query
├── Processing Engines
│   ├── Apache Flink (default)
│   │   └── Implementazione delle Query 1, 2 e 3
│   └── Kafka Streams (opzionale)
│       └── Implementazione delle Query 1 e 2
├── Results Processing
│   └── Consumer
│       ├── Sottoscrizione ai topic di risultati
│       ├── Elaborazione dei risultati
│       └── Invio risultati al LOCAL-CHALLENGER per validazione
└── Monitoring & Visualization (opzionale)
    ├── Prometheus
    │   ├── Raccolta metriche di sistema personalizzate per Flink
    │   └── Monitoraggio prestazioni in tempo reale
    └── Grafana
        └── Dashboard per visualizzazione metriche
```

### Ambiente di Deployment
L'infrastruttura è stata orchestrata tramite Docker Compose, con i seguenti componenti principali:

1. **Cluster Kafka**.
   - Broker Kafka per la gestione dei topic
   - ZooKeeper per il coordinamento del cluster

2. **Cluster Flink**.
   - JobManager: Coordinamento dei job di processing
   - TaskManager: Esecuzione dei task di elaborazione
   - Flink Client: Sottomissione dei job al cluster

3. **Kafka Streams**.
   - Deployment come singolo container, a differenza di Flink che richiede un cluster dedicato.

4. **LOCAL-CHALLENGER**.
   - Simula l'ambiente di produzione
   - Valida i risultati delle query e calcola le metriche di performance

5. **Container di Supporto**.
   - Producer: Recupera dati dal LOCAL-CHALLENGER e li pubblica su Kafka
   - Consumer: Legge i risultati da Kafka e li invia al LOCAL-CHALLENGER
   - Prometheus: Monitoraggio e raccolta metriche personalizzate
   - Grafana: Visualizzazione delle metriche personalizzate 

### Query implementate
Il sistema implementa le tre query richieste dalla traccia e la modalità `--short` esegue solo le prime due (utile per confrontare le prestazioni tra Flink e Kafka Streams).