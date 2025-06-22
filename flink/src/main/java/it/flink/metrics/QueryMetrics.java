package it.flink.metrics;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Gauge;

import com.codahale.metrics.SlidingWindowReservoir;

import it.flink.model.TileLayerData;

/** Classe che raccoglie e registra metriche di performance personalizzate per le query.
 *  Misura latenza, throughput e numero totale di elementi processati.
 */
public class QueryMetrics extends RichMapFunction<TileLayerData, TileLayerData> {
    private static final long THROUGHPUT_WINDOW_MS = 1000;

    public enum MetricType {
        QUERY1,
        QUERY2
    }

    private final String queryName;
    private final MetricType metricType;
    private final boolean isProfilingEnabled;

    // Metriche flink
    private transient Histogram latencyHistogram;
    private transient Counter elementCounter;

    // Stato per il calcolo del throughput
    private long lastTimestamp;
    private int elementsInInterval;
    private double throughputValue;
    
    // Nuovo costruttore con parametro per abilitare/disabilitare profiling
    public QueryMetrics(String queryName, MetricType metricType, boolean isProfilingEnabled) {
        this.queryName = queryName;
        this.metricType = metricType;
        this.isProfilingEnabled = isProfilingEnabled;
    }

    @Override
    public void open(OpenContext ctx) {
        if (!isProfilingEnabled) {
            return;
        }

        var metricGroup = getRuntimeContext()
            .getMetricGroup()
            .addGroup("metrics")
            .addGroup(queryName);

        // Isotgramma per la latenza con SlidingWindowReservoir
        com.codahale.metrics.Histogram dropwizardHistogram =
            new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100));
        this.latencyHistogram = metricGroup.histogram("latency_ms",
            new DropwizardHistogramWrapper(dropwizardHistogram));
        
        // Contatore per il numero totale di elementi processati
        this.elementCounter = metricGroup.counter("elements_total");
            
        // Variabili per il throughput
        this.lastTimestamp = System.currentTimeMillis();
        this.elementsInInterval = 0;
        this.throughputValue = 0.0;
        
        // Gauge per il throughput (elementi/secondo)
        metricGroup.gauge("throughput_elements_per_second",
            (Gauge<Double>) () -> throughputValue);
    }

    @Override
    public TileLayerData map(TileLayerData tile) throws Exception {
        if (!isProfilingEnabled) {
            return tile;
        }

        long now = System.currentTimeMillis();
    
        // Aggiornamento contatori
        elementCounter.inc();
        elementsInInterval++;
        
        // Calcola la latenza in base al tipo di query
        switch (metricType) {
            case QUERY1:
                if (tile.processingStartTime > 0) {
                    long latency = now - tile.processingStartTime;
                    latencyHistogram.update(latency);
                }
                tile.q1EndTime = now;
                break;

            case QUERY2:
                if (tile.q2StartTime > 0) {
                    long latency = now - tile.q2StartTime;
                    latencyHistogram.update(latency);
                }
                tile.q2EndTime = now;
                break;
        }
        
        // Aggiorna il throughput quando la finestra è completa
        if (now - lastTimestamp >= THROUGHPUT_WINDOW_MS) {
            double seconds = (now - lastTimestamp) / 1000.0;
            throughputValue = elementsInInterval / seconds;
            
            // Reset per la prossima finestra
            elementsInInterval = 0;
            lastTimestamp = now;
        }
        
        return tile;
    }
}