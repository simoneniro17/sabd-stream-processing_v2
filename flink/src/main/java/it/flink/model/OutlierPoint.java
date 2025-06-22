package it.flink.model;

/** Rappresenta un singolo punto classificato come outlier
 * (i.e. con deviazione di temperatura locale > 6000).
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
