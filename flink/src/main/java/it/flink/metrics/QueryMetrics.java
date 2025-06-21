package it.flink.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import com.codahale.metrics.SlidingWindowReservoir;
import it.flink.model.TileLayerData;

public class QueryMetrics extends RichMapFunction<TileLayerData, TileLayerData> {
    private final String queryName;
    private transient Histogram latencyHistogram;
    private transient Counter elementCounter;
    private long lastTimestamp;
    private int elementsInInterval;
    private double throughputValue;
    private final MetricType metricType;
    private static final long THROUGHPUT_WINDOW_MS = 1000;
    private final boolean isProfilingEnabled;
    
    public enum MetricType {
        QUERY1,
        QUERY2
    }

    // Costruttore originale che abilita sempre il profiling
    public QueryMetrics(String queryName, MetricType metricType) {
        this(queryName, metricType, true);
    }
    
    // Nuovo costruttore con parametro per abilitare/disabilitare profiling
    public QueryMetrics(String queryName, MetricType metricType, boolean isProfilingEnabled) {
        this.queryName = queryName;
        this.metricType = metricType;
        this.isProfilingEnabled = isProfilingEnabled;
    }

    @Override
    public void open(OpenContext ctx) {
        // Registra metriche solo se il profiling è abilitato
        if (isProfilingEnabled) {
            com.codahale.metrics.Histogram dropwizardHistogram =
                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(100));

            this.latencyHistogram = getRuntimeContext()
                .getMetricGroup()
                .addGroup("metrics")
                .addGroup(queryName)
                .histogram("latency_ms", new DropwizardHistogramWrapper(dropwizardHistogram));
                
            this.elementCounter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("metrics")
                .addGroup(queryName)
                .counter("elements_total");
                
            this.lastTimestamp = System.currentTimeMillis();
            this.elementsInInterval = 0;
            this.throughputValue = 0.0;
            
            // Aggiungi Gauge per throughput (elementi/secondo)
            getRuntimeContext()
                .getMetricGroup()
                .addGroup("metrics")
                .addGroup(queryName)
                .gauge("throughput_elements_per_second", new Gauge<Double>() {
                    @Override
                    public Double getValue() {
                        return throughputValue;
                    }
                });
        }
    }

    @Override
    public TileLayerData map(TileLayerData tile) throws Exception {
        // Calcola metriche solo se il profiling è abilitato
        if (isProfilingEnabled) {
            long now = System.currentTimeMillis();
        
        
            // Incrementa contatori
            elementCounter.inc();
            elementsInInterval++;
            
            // Calcola la latenza in base al tipo di metrica
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
            
            // Aggiorna il throughput ogni THROUGHPUT_WINDOW_MS
            if (now - lastTimestamp >= THROUGHPUT_WINDOW_MS) {
                // Calcola elementi al secondo
                double seconds = (now - lastTimestamp) / 1000.0;
                throughputValue = elementsInInterval / seconds;
                
                // Reset contatori
                elementsInInterval = 0;
                lastTimestamp = now;
            }
        }
        
        return tile;
    }
}