package se.meltwater.examples;


import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class Benchmarks {

    private static float chanceNewNode = 1.01f;
    private static long bytesPerGigaByte = 1073741824;

    public static void benchmarkInsertions() throws IOException, InterruptedException {
        int log2m = 7;
        int h = 3;
        int edgesToAdd = 5000000;
        int bulkSize = 10000;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);

        DANF danf = runHyperBoll(log2m, h, graph);

        PrintWriter writer = new PrintWriter("benchmarks.data");

        int added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        Edge[] edges = new Edge[bulkSize];

        while(System.in.available() == 0 && added < edgesToAdd) {

            generateEdges(graph, bulkSize, edges);

            danf.addEdges(edges);
            added += bulkSize;

            long currentTime = System.currentTimeMillis();
            printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime);
            lastTime = currentTime;
        }

        writer.close();
    }

    private static void printAndLogStatistics(PrintWriter writer, int added, long lastTime, long startTime, int bulkSize, long currentTime) {
        long elapsedTime = currentTime - lastTime;
        float timePerBulkSeconds = elapsedTime / 1000.0f;
        float dps = bulkSize / timePerBulkSeconds;
        float heapSize = Runtime.getRuntime().totalMemory() / (float)bytesPerGigaByte;
        float elapsedTimeSinceStart = (currentTime - startTime) / 1000.0f;
        float addedInMillions = (float)added / 1000000;

        System.out.print("Total nr edges: " + added + ". ");
        System.out.print("Added " + bulkSize + " edges. ");
        System.out.print("Time per bulk: " + timePerBulkSeconds + "s. ");
        System.out.print("DPS: " + dps + ". ");
        System.out.print("Heap size: " + heapSize + " Gb. ");
        System.out.println("Total time: " + elapsedTimeSinceStart + "s.");

        writer.println(addedInMillions + " " + dps + " " + heapSize + " " + elapsedTimeSinceStart);
    }

    private static void generateEdges(SimulatedGraph graph, int bulkSize, Edge[] edges) {
        long maxNode = graph.getNumberOfNodes();
        long maxNewNodes = (long)(maxNode * chanceNewNode) + 10;

        for (int i = 0; i < bulkSize; i++) {
            long from = ThreadLocalRandom.current().nextLong(maxNewNodes );
            long to = ThreadLocalRandom.current().nextLong(maxNewNodes );
            edges[i] = new Edge(from, to);
        }
    }

    private static DANF runHyperBoll(int log2m, int h, SimulatedGraph graph) throws IOException {
        DynamicVertexCover dvc = new DynamicVertexCover(graph);
        DANF danf = new DANF(dvc, h, graph);

        HyperBoll hyperBoll = new HyperBoll(graph, log2m);
        hyperBoll.init();
        for (int i = 1; i <= h; i++) {
            hyperBoll.iterate();
            danf.addHistory(hyperBoll.getCounter(), i);
        }
        hyperBoll.close();

        return danf;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Benchmarks.benchmarkInsertions();
    }
}
