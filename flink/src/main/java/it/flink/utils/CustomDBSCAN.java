package it.flink.utils;

import java.util.*;

import it.flink.model.OutlierPoint;

/** Implementazione di DBSCAN ispirata a quella di scikit-learn.
 *  Raggruppa punti che sono vicini tra loro in base a una distanza epsilon (eps)
 *  e un numero minimo di punti (minPts) richiesti per formare un cluster.
 */
public class CustomDBSCAN {
    // Parametri algoritmo
    private final double eps;       // Raggio entro cui cercare i vicini
    private final int minPts;       // Numero minimo di punti per formare un cluster
    
    private List<OutlierPoint> points;  // Punti da raggruppare
    private int[] labels;               // Etichette dei cluster (-1 = rumore)
    
    public CustomDBSCAN(double eps, int minPts) {
        this.eps = eps;
        this.minPts = minPts;
    }
    
    /** Esegue il clustering sui punti forniti */
    public int[] fit(List<OutlierPoint> points) {
        this.points = points;
        int n = points.size();
        labels = new int[n];

        // Inizializza tutte le etichette a -1 (non visitato)
        Arrays.fill(labels, -1);
        
        int clusterID = 0;
        
        // Scansione di tutti i punti
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
            expandCluster(i, neighbors, clusterID);
            clusterID++;
        }
        
        return labels;
    }

    /** Espande un cluster a partire da un punto e i suoi vicini */
    private void expandCluster(int pointIdx, List<Integer> neighbors, int clusterID) {
        List<Integer> queue = new ArrayList<>(neighbors);
        
        for (int i = 0; i < queue.size(); i++) {
            int idx = queue.get(i);
            
            // Salta punti già etichettati come parte di un cluster
            if (labels[idx] > -1) continue;
            
            // Etichetta il punto come parte del cluster
            labels[idx] = clusterID;
            
            // Trova i vicini di questo punto
            List<Integer> newNeighbors = getNeighbors(idx);
            
            // Se questo è un punto core, aggiungi i suoi vicini non visitati alla coda
            if (newNeighbors.size() >= minPts) {
                for (int newIdx : newNeighbors) {
                    if (!queue.contains(newIdx) && labels[newIdx] == -1) {
                        queue.add(newIdx);
                    }
                }
            }
        }
    }
    
    /** Trova tutti i punti entro eps distanza dal punto specificato */
    private List<Integer> getNeighbors(int pointIdx) {
        List<Integer> neighbors = new ArrayList<>();
        OutlierPoint p1 = points.get(pointIdx);
        
        // Distanza euclidea tra da ogni altro punto
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