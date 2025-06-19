package it.flink.utils;

import java.util.*;

import it.flink.model.OutlierPoint;

public class CustomDBSCAN {
    
    private double eps;
    private int minPts;
    private List<OutlierPoint> points;
    private int[] labels;
    
    public CustomDBSCAN(double eps, int minPts) {
        this.eps = eps;
        this.minPts = minPts;
    }
    
    public int[] fit(List<OutlierPoint> points) {
        this.points = points;
        int n = points.size();
        labels = new int[n];
        Arrays.fill(labels, -1); // -1 indica punti non visitati
        
        int clusterID = 0;
        
        // Per ogni punto non visitato
        for (int i = 0; i < n; i++) {
            if (labels[i] != -1) continue; // Punto già visitato
            
            // Trova vicini
            List<Integer> neighbors = getNeighbors(i);
            
            // Se non ha abbastanza vicini, è rumore
            if (neighbors.size() < minPts) {
                labels[i] = -1;
                continue;
            }
            
            // Altrimenti è un nuovo cluster
            labels[i] = clusterID;
            
            // Espandi il cluster
            expandCluster(i, neighbors, clusterID);
            clusterID++;
        }
        
        return labels;
    }
    
    private void expandCluster(int pointIdx, List<Integer> neighbors, int clusterID) {
        List<Integer> queue = new ArrayList<>(neighbors);
        
        for (int i = 0; i < queue.size(); i++) {
            int idx = queue.get(i);
            
            // Salta punti già etichettati come parte di un cluster
            if (labels[idx] > -1) continue;
            
            // Etichetta il punto come parte del cluster
            labels[idx] = clusterID;
            
            // Trova vicini del vicino
            List<Integer> newNeighbors = getNeighbors(idx);
            
            // Se ha abbastanza vicini, aggiungi alla coda
            if (newNeighbors.size() >= minPts) {
                for (int newIdx : newNeighbors) {
                    if (!queue.contains(newIdx) && labels[newIdx] == -1) {
                        queue.add(newIdx);
                    }
                }
            }
        }
    }
    
    private List<Integer> getNeighbors(int pointIdx) {
        List<Integer> neighbors = new ArrayList<>();
        OutlierPoint p1 = points.get(pointIdx);
        
        for (int i = 0; i < points.size(); i++) {
            OutlierPoint p2 = points.get(i);
            double distance = Math.sqrt(Math.pow(p1.x - p2.x, 2) + Math.pow(p1.y - p2.y, 2));
            
            if (distance <= eps) {
                neighbors.add(i);
            }
        }
        
        return neighbors;
    }
}