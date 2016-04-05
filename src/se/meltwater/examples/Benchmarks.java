package se.meltwater.examples;


import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class Benchmarks {

    private static float chanceNewNode = 1.05f;
    private static long bytesPerGigaByte = 1024 * 1024 * 1024;

    public static void benchmarkDVCInsertions() throws IOException {
        SimulatedGraph graph = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        final int edgesToAdd = 100000000;
        final int bulkSize =     1000000;

        int added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        Edge[] edges = new Edge[bulkSize];
        PrintWriter writer = new PrintWriter("benchmarksdvc.data");

        while(System.in.available() == 0 && added < edgesToAdd) {
            generateEdges(graph.getNumberOfNodes(), bulkSize, edges);
            for (int i = 0; i < bulkSize; i++) {
                dvc.insertEdge(edges[i]);
            }
            graph.addEdges(edges);
            added += bulkSize;

            long currentTime = System.currentTimeMillis();
            printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime);
            lastTime = currentTime;
        }

        writer.close();
    }

    public static void benchmarkDVCDeletions() throws IOException {
        SimulatedGraph graph   = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        final long maxNode =       100000;
        final int edgesToAdd =   10000000;
        final int bulkSize =        10000;

        int added = 0;

        ArrayList<Edge> edgeList = new ArrayList<>(edgesToAdd);
        Edge[] edges = new Edge[bulkSize];
        PrintWriter writer = new PrintWriter("benchmarksdvcdelete.data");

        while(System.in.available() == 0 && added < edgesToAdd) {
            generateEdges(maxNode, bulkSize, edges);
            for (int i = 0; i < bulkSize; i++) {
                dvc.insertEdge(edges[i]);
            }
            graph.addEdges(edges);
            edgeList.addAll(Arrays.asList(edges));
            added += bulkSize;
        }

        int removed = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        System.out.println("Graph nodes: " + graph.getNumberOfNodes() + ", Arcs: " + graph.getNumberOfArcs());

        Collections.shuffle(edgeList);
        edges = edgeList.toArray(new Edge[edgeList.size()]);

        while(removed < added) {
            for (int i = 0; i < bulkSize; i++) {
                Edge edgeToRemove = edges[i + removed];
                graph.deleteEdge(edgeToRemove);
                dvc.deleteEdge(edgeToRemove);
            }

            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - lastTime;
            float timePerBulkSeconds = elapsedTime / 1000.0f;
            float dps = bulkSize / timePerBulkSeconds;

            float heapSize = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (float)bytesPerGigaByte;

            float elapsedTimeSinceStart = (currentTime - startTime) / 1000.0f;
            float addedInMillions = (float)added / 1000000;

            System.out.print("Total nr edges: " + added + ". ");
            System.out.print("Added " + bulkSize + " edges. ");
            System.out.print("Time per bulk: " + timePerBulkSeconds + "s. ");
            System.out.print("DPS: " + dps + ". ");
            System.out.print("Heap size: " + heapSize + " Gb. ");
            System.out.println("Total time: " + elapsedTimeSinceStart + "s.");

            writer.println(addedInMillions + " " + dps + " " + heapSize + " " + elapsedTimeSinceStart + " " + graph.getNumberOfArcs());

            lastTime = currentTime;

            System.out.print("DVC Size: " + dvc.getVertexCoverSize() + " ");
            System.out.println("Graph arcs: " + graph.getNumberOfArcs());

            removed += bulkSize;
        }

        writer.close();
    }

    public static void benchmarkInsertions() throws IOException, InterruptedException {
        int log2m = 7;
        int h = 3;
        int edgesToAdd = 5000000;
        int bulkSize = 50000;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);

        DANF danf = runHyperBoll(log2m, h, graph);

        PrintWriter writer = new PrintWriter("benchmarksSimon.data");

        int added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        Edge[] edges = new Edge[bulkSize];

        while(System.in.available() == 0 && added < edgesToAdd) {
            generateEdges(graph.getNumberOfNodes(), bulkSize, edges);

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

        float heapSize = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (float)bytesPerGigaByte;

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

    private static void generateEdges(long currentMaxNode, int bulkSize, Edge[] edges) {
        long maxNewNodes = (long)(currentMaxNode * chanceNewNode) + 100;

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
        //Benchmarks.benchmarkInsertions();
        //Benchmarks.benchmarkDVCInsertions();
        Benchmarks.benchmarkDVCDeletions();
    }
}
