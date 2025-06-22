package it.flink.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import com.codahale.metrics.SlidingWindowReservoir;
import it.flink.model.TileLayerData;

public class Query1TimedMapFunction extends RichMapFunction<TileLayerData, TileLayerData> {
  private transient Histogram histogram;

  @Override
  public void open(OpenContext ctx) {
    com.codahale.metrics.Histogram dropwizardHistogram =
      new com.codahale.metrics.Histogram(new SlidingWindowReservoir(30));

    this.histogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("query1ProcessingLatency", new DropwizardHistogramWrapper(dropwizardHistogram));
  }

  @Override
  public TileLayerData map(TileLayerData tile) throws Exception {
    long now = System.currentTimeMillis();
    long latency = now - tile.processingStartTime;
    histogram.update(latency);  // Registra la latenza
    // Aggiorna il timestamp per misurare il tempo di Q1
    tile.q1EndTime = now;
    return tile;
  }
}