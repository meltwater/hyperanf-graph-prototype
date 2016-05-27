package se.meltwater.benchmarks;

import se.meltwater.graph.Edge;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by johan on 2016-05-27.
 */
public class BenchmarkUtils {

    final public static String graphFolder = "testGraphs/";
    final public static String dataFolder = "files/";

    final public static long bytesPerGigaByte = 1024 * 1024 * 1024;

    /**
     * Generates a bulk of random edges.
     * @param maxNewNode The max node id of the generated edges
     * @param bulkSize The number of edges to generate
     */
    public static Edge[] generateEdges(long maxNewNode, int bulkSize) {
        Edge[] edges = new Edge[bulkSize];

        for (int i = 0; i < bulkSize; i++) {
            long from = ThreadLocalRandom.current().nextLong(maxNewNode );
            long to = ThreadLocalRandom.current().nextLong(maxNewNode );
            edges[i] = new Edge(from, to);
        }

        return edges;
    }

    /**
     * Returns the current time as date
     * @return
     */
    public static String getDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }
}
