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
   docker image load -i gc25docker.tar
   cd ..
   ```

### Utilizzo del main

### Esecuzione da riga di comando

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

5. **Container di Supporto**.
   - Producer: Recupera dati dal LOCAL-CHALLENGER e li pubblica su Kafka
   - Consumer: Legge i risultati da Kafka e li invia al LOCAL-CHALLENGER
   - Prometheus: Monitoraggio e raccolta metriche personalizzate
   - Grafana: Visualizzazione delle metriche personalizzate 