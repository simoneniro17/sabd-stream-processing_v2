package it.kafkastreams.processing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

import it.kafkastreams.model.OutlierPoint;
import it.kafkastreams.model.TileLayerData;

public class Query2 {
    private static final int EMPTY_THRESHOLD = 5000;
    private static final int SATURATED_THRESHOLD = 65000;
    private static final int OUTLIER_THRESHOLD = 6000;
    private static final int DISTANCE_FACTOR = 2;
    private static final int TOP_OUTLIERS_COUNT = 5;

    public static TileLayerData analyzeWindow(Deque<TileLayerData> window) {
        if (window.size() < 3) return null;

        List<TileLayerData> layers = new ArrayList<>(window);
        layers.sort(Comparator.comparingInt(t -> t.layerId));
        TileLayerData currentLayer = layers.get(2);

        List<OutlierPoint> outliers = findOutliers(layers);

        return createOutput(currentLayer, outliers);
    }

    private static List<OutlierPoint> findOutliers(List<TileLayerData> layers) {
        List<OutlierPoint> outliers = new ArrayList<>();

        int[][][] temp3d = createTemperature3DMatrix(layers);
        int depth = temp3d.length;
        int height = temp3d[0].length;
        int width = temp3d[0][0].length;

        int[][] current = temp3d[depth - 1];

        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                int temp = current[y][x];
                if (temp <= EMPTY_THRESHOLD || temp >= SATURATED_THRESHOLD) continue;

                NeighborStats stats = calculateNeighborStats(temp3d, depth, y, x);
                double deviation = Math.abs(stats.getCloseAvg() - stats.getDistantAvg());

                if (deviation > OUTLIER_THRESHOLD) {
                    outliers.add(new OutlierPoint(x, y, deviation));
                }
            }
        }

        outliers.sort((o1, o2) -> Double.compare(o2.deviation, o1.deviation));
        return outliers;
    }

    private static int[][][] createTemperature3DMatrix(List<TileLayerData> layers) {
        int depth = layers.size();
        int[][][] matrix = new int[depth][][];
        for (int i = 0; i < depth; i++) {
            matrix[i] = layers.get(i).temperatureMatrix;
        }
        return matrix;
    }

    private static class NeighborStats {
        double closeSum = 0;
        int closeCount = 0;
        double distantSum = 0;
        int distantCount = 0;

        double getCloseAvg() {
            return closeCount > 0 ? closeSum / closeCount : 0.0;
        }

        double getDistantAvg() {
            return distantCount > 0 ? distantSum / distantCount : 0.0;
        }
    }

    private static NeighborStats calculateNeighborStats(int[][][] temp3d, int depth, int y, int x) {
        NeighborStats stats = new NeighborStats();
        int currentDepth = depth - 1;

        for (int j = -DISTANCE_FACTOR; j <= DISTANCE_FACTOR; j++) {
            for (int i = -DISTANCE_FACTOR; i <= DISTANCE_FACTOR; i++) {
                for (int d = 0; d < depth; d++) {
                    int md = Math.abs(i) + Math.abs(j) + Math.abs(currentDepth - d);
                    if (md <= DISTANCE_FACTOR) {
                        stats.closeSum += getValue(temp3d, d, y + j, x + i);
                        stats.closeCount++;
                    }
                }
            }
        }

        for (int j = -2 * DISTANCE_FACTOR; j <= 2 * DISTANCE_FACTOR; j++) {
            for (int i = -2 * DISTANCE_FACTOR; i <= 2 * DISTANCE_FACTOR; i++) {
                for (int d = 0; d < depth; d++) {
                    int md = Math.abs(i) + Math.abs(j) + Math.abs(currentDepth - d);
                    if (md > DISTANCE_FACTOR && md <= 2 * DISTANCE_FACTOR) {
                        stats.distantSum += getValue(temp3d, d, y + j, x + i);
                        stats.distantCount++;
                    }
                }
            }
        }

        return stats;
    }

    private static double getValue(int[][][] image, int d, int y, int x) {
        if (d < 0 || d >= image.length ||
            y < 0 || y >= image[d].length ||
            x < 0 || x >= image[d][y].length) {
            return 0.0;
        }
        return image[d][y][x];
    }

    private static TileLayerData createOutput(TileLayerData currentLayer, List<OutlierPoint> allOutliers) {
        String[] points = new String[TOP_OUTLIERS_COUNT];
        String[] deviations = new String[TOP_OUTLIERS_COUNT];
        Arrays.fill(points, "()");
        Arrays.fill(deviations, "");

        List<OutlierPoint> topOutliers = allOutliers.size() > TOP_OUTLIERS_COUNT ?
                allOutliers.subList(0, TOP_OUTLIERS_COUNT) : allOutliers;

        for (int i = 0; i < topOutliers.size(); i++) {
            OutlierPoint point = topOutliers.get(i);
            points[i] = String.format("(%d;%d)", point.y, point.x);
            deviations[i] = String.format("%.2f", point.deviation);
        }

        currentLayer.addOutlierResults(
            allOutliers,
            points[0], deviations[0],
            points[1], deviations[1],
            points[2], deviations[2],
            points[3], deviations[3],
            points[4], deviations[4]
        );

        return currentLayer;
    }
}