package it.flink.model;

import java.util.List;

public class Cluster {
    private final List<OutlierPoint> points;

    public Cluster(List<OutlierPoint> points) {
        this.points = points;
    }

    public List<OutlierPoint> getPoints() {
        return points;
    }

    public double getCentroidX() {
        return points.stream().mapToDouble(p -> p.x).average().orElse(0);
    }

    public double getCentroidY() {
        return points.stream().mapToDouble(p -> p.y).average().orElse(0);
    }

    public int getCount() {
        return points.size();
    }

    @Override
    public String toString() {
        return String.format("{'x': %.2f; 'y': %.2f; 'count': %d}", getCentroidY(), getCentroidX(), getCount());
    }
}
