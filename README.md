# Progetto 2 - Analisi Real-time di Difetti nella Produzione Manifatturiera

## Obiettivo Generale del Progetto
Implementare un sistema di monitoraggio che analizza in tempo reale un flusso di **immagini di tomografia ottica** (***OT images***) di un oggetto in fase di creazione per predire la presenza di difetti in alcune aree dell'oggetto. Le immagini sono sottomesse come uno stream continuo di dati, simulando il monitoraggio dell'oggetto durante la sua stampa.

### Contesto Tecnologico
- **Laser Powder Bed Fusion (L-PBF) manufacturing**: Tecnica per realizzare parti metalliche a partire da un letto di polveri fuse tramite laser.
- **Problema**: Soggetta a difetti dovuti a impurità delle polveri o errori di calibrazione.
- **Soluzione**: Monitoraggio in tempo reale per bloccare la stampa se c'è alta probabilità di difetti, evitando sprechi di tempo e materiali.


## Dataset e Struttura Dati

### Caratteristiche del Dataset
- **Fonte**: Container LOCAL-CHALLENGER fornito dal GrandChallenge
- **Formato**: Stream di immagini tomografiche ottiche
- **Contenuto**: Per ogni punto P=(x,y), fornisce la temperatura T(P) misurata layer per layer (coordinata Z).
- **Elaborazione**: Lo stream consegna le immagini un livello alla volta e ciascuno di essi è suddiviso in ***tile*** di uguale dimensione.

### Struttura degli Oggetti Stream
Ogni layer è ricevuto come tile multipli.

Ogni tupla dello stream contiene i seguenti campi:
- **seq_id**: Numero identificativo univoco dello stream
- **print_id**: Identificativo del pezzo in corso di stampa
- **tile_id**: Identificativo del tile all'interno del layer
- **layer**: Identificativo del layer (coordinata Z)
- **tiff**: Dati binari rappresentanti il tile in formato TIFF (16 bit ad alta risoluzione per i dati sulla temperatura)

### API del LOCAL-CHALLENGER
Il dataset è fornito dal container LOCAL-CHALLENGER, che espone un'interfaccia REST con 5 endpoint:
- `/create`: Crea nuovo benchmark
- `/start`: Avvia la generazione del flusso di dati (i.e. avvia il benchmark)
- `/next_batch`: Richiede il batch successivo di dati
- `/result`: Invia il risultato al container per ottenere le statistiche
- `/end`: Termina il benchmark


## Pipeline di Elaborazione con Apache Flink

### Architettura della Pipeline
La pipeline di processamento richiede **4 stadi sequenziali**:
1. **Analisi di saturazione** (Query 1)
2. **Creazione finestre** (Query 2 - Parte 1)
3. **Analisi degli outlier** (Query 2 - Parte 2)
4. **Clustering degli outlier** (Query 3)

**Nota**: Le query non vanno eseguite indipendentemente, ma in una pipeline di processamento.


## Query Richieste

### Query 1 - Analisi di Saturazione
**Obiettivo**: Identificare punti critici per temperatura

**Logica**:
- Per ogni tile, identificare tutti i punti che hanno una temperatura:
  - $< 5000$ (aree vuote da escludere dalle query successive)
  - $> 65000$ (punti saturati, che potrebbero indicare difetti). Dovrebbero essere riportati, ma non inclusi nelle analisi delle query successive.

**Header CSV di output**:
```
seq_id, print_id, tile_id, saturated
```
Dove `saturated` è il numero di punti saturati.

### Query 2 - Finestre Scorrevoli e Analisi Outlier
**Obiettivo**: Analizzare evoluzione temperatura tra layer consecutivi

**Logica**:
- Per ogni tile, mantenere finestra scorrevole degli **ultimi 3 layer**, abilitando l'analisi dell'evoluzione della temperatura tra layer consecutivi. La finestra sarà quindi count-based con uno sliding di 1.

- Per ogni punto P nel layer più recente, calcolare **deviazione di temperatura locale**, definita coma la **differenza assoluta tra**:
  1. **Temperatura media dei punti vicini prossimi** a P (i.e. tutti i punti con distanza Manhattan $0 \leq d \leq 2$ da P, considerando i tre layer)
  2. **Temperatura media dei punti vicini esterni** a P (i.e. tutti i punti con distanza Manhattan $2 < d \leq 4$ da P, considerando i tre layer)

- Classificare un punto come ***outlier*** se la sua deviazione di temperatura locale supera la soglia di 6000.

- La query deve restituire i 5 punti con la deviazione più elevata per ogni tile e finestra.

**Header CSV di output**:
```
seq_id, print_id, tile_id, P1, dP1, P2, dP2, P3, dP3, P4, dP4, P5, dP5
```
dove P1-P5 sono i 5 punti con deviazione maggiore e dP1-dP5 le rispettive deviazioni.

### Query 3 - Clustering con DBSCAN
**Obiettivo**: Clusterizzare punti outlier identificati in Q2

**Logica**:
- Utilizzare i punti outlier identificati in Query 2 per eseguire un clustering basato su DBSCAN, usando la distanza euclidea come metrica.

**Header CSV di output**:
```
seq_id, print_id, tile_id, saturated, centroids
```
Dove:
- `saturated`: Conteggio punti saturati
- `centroids`: Lista centroidi cluster (coordinate x,y e dimensione cluster)

**Nota Importante**: L'output di Q3 deve essere fornito al LOCAL-CHALLENGER per ottenere, alla fine del benchmark, le metriche di prestazioni direttamente dal container.

### Metriche da Valutare
Per ogni query occorre misurare ed inserire sul report/presentazione le seguenti metriche sulla piattaforma di riferimento:
- **Latenza**: Tempo per processare ogni tile
- **Throughput**: Numero di tile processati per unità di tempo

### Modalità di Misurazione
- Utilizzare API di monitoraggio Flink o inserire punti di calcolo manuali nel codice
- **Importante**: Eseguire multiple run per affidabilità statistica
- Riportare anche deviazione standard

## Requisiti per Team
### Team da 3 Studenti
- Implementare Query 1, 2 e 3
- Valutare metriche di prestazioni
- **Plus**: Studio di una tra le seguenti ottimizzazioni:
  1. **Pipeline processing**: Ottimizzare elaborazione attraverso i 4 stadi
  2. **Parallel processing**: Processare tile in parallelo per identificare difetti su livello specifico
  3. **Application logic**: Sfruttare simmetria spaziale nel calcolo deviazione temperatura locale

### Parte Opzionale
- Usare **Kafka Streams** o **Spark Streaming** per Query 1 e 2
- Confrontare prestazioni con Flink