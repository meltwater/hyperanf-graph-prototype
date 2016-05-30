package it.unimi.dsi.big.webgraph.benchmarks;

import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.MutableGraph;
import se.meltwater.utils.Utils;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class BenchmarkUtils {

    final public static String graphFolder = "files/";
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

    /**
     * Prints general statistics in benchmarks.
     * @param writer
     * @param nrModified
     * @param lastTime
     * @param startTime
     * @param bulkSize
     * @param currentTime
     * @param graph
     * @param objectToMeasureMemory
     */
    public static void printAndLogStatistics(PrintWriter writer, int nrModified, long lastTime, long startTime, int bulkSize, long currentTime, MutableGraph graph, Object objectToMeasureMemory) {
        long elapsedTime = currentTime - lastTime;
        float timePerBulkSeconds = elapsedTime / 1000.0f;
        float dps = bulkSize / timePerBulkSeconds;

        float heapSize = Utils.getMemoryUsage(objectToMeasureMemory) / (float)bytesPerGigaByte;

        float elapsedTimeSinceStart = (currentTime - startTime) / 1000.0f;
        float modifiedInMillions = (float)nrModified / 1000000;

        System.out.print("Total nr edges: " + nrModified + ". ");
        System.out.print("Modified " + bulkSize + " edges. ");
        System.out.print("Time per bulk: " + timePerBulkSeconds + "s. ");
        System.out.print("DPS: " + dps + ". ");
        System.out.print("NrArcs: " + 0/*graph.getNumberOfArcs()*/ + " ");
        System.out.print("NrNodes: " + graph.numNodes() + " ");
        System.out.println("Total time: " + elapsedTimeSinceStart + "s.");

        writer.println(modifiedInMillions + " " + dps + " " + heapSize + " " + elapsedTimeSinceStart  + " " + /*graph.getNumberOfArcs()*/0 + " " + graph.numNodes());
        writer.flush();
    }
}
