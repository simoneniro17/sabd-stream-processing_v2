1. Installare maven
2. Installare le estensioni di Java e di Maven in vscode


Per compilare: 
```
cd flink
mvn clean package
```

Il comando precedente crea il JAR (flink-1.0-SNAPSHOT.jar), che viene copiato automaticamente nella cartella jobs.

Per lanciare il job da dentro il container:
```bash
flink run --jobmanager jobmanager:8081 -c it.flink.StreamingJob /opt/flink/jobs/flink-1.0-SNAPSHOT.jar
```

Per leggere i risultati da Kafka
```bash
docker exec consumer python /app/consumer.py --topic queryN-results
```