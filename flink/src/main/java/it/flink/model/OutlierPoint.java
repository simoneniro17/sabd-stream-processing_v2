package it.flink.model;

public class OutlierPoint {
    public final int x;
    public final int y;
    public final double deviation;

    public OutlierPoint(int x, int y, double deviation) {
        this.x = x;
        this.y = y;
        this.deviation = deviation;
    }
}
