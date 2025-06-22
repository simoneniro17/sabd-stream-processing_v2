package it.kafkastreams.model;

/** Rappresenta un singolo punto outlier (i.e. con deviazione di temperatura locale
 * maggiore di 6000) con le sue coordinate (x, y) e la deviazione calcolata.
 * TODO:Le coordinate sono relative alla matrice o alla immagine??
*/
public class OutlierPoint {
    public final int x;
    public final int y;
    public final double deviation;

    public OutlierPoint(int x, int y, double deviation) {
        this.x = x;
        this.y = y;
        this.deviation = deviation;
    }

    @Override
    public String toString() {
        return String.format("(%d,%d):%.2f", x, y, deviation);
    }
}
