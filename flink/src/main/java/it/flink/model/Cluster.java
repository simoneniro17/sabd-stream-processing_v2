package it.flink.model;

import java.util.List;

public class Cluster {
    
    List<OutlierPoint> points;

    public Cluster(List<OutlierPoint> points) {
        this.points = points;
    }

}
