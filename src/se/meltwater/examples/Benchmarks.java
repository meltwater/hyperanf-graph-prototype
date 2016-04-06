package se.meltwater.examples;


import it.unimi.dsi.big.webgraph.ImmutableGraph;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Remove copy-paste!
 */
public class Benchmarks {

    private static float chanceNewNode = 1.05f;
    private static long bytesPerGigaByte = 1024 * 1024 * 1024;

    private static int added = 0;
    private static long lastTime;


    public static void benchmarkUnionVsStored() throws IOException {
        ImmutableGraphWrapper graphUnioned = new ImmutableGraphWrapper(ImmutableGraph.load("testGraphs/in-2004"));
        ImmutableGraphWrapper graphStored = new ImmutableGraphWrapper(ImmutableGraph.load("testGraphs/in-2004"));

        long maxNode = 1000000;
        int bulkSize = 1000;

        Edge[] edges = new Edge[bulkSize];
        int iteration = 0;
        int maxIteration = 50;
        PrintWriter writer = new PrintWriter("unionVSStored.data");
        writer.println("#Iteration UnionedTimems StoredTimedms");

        while (iteration++ < maxIteration) {
            generateEdges(maxNode, bulkSize, edges);

            long startTime = System.currentTimeMillis();
            graphUnioned.addEdgesUnioned(edges);
            graphUnioned.iterateAllEdges(edge -> {
                return null;
            });
            long unionTime = System.currentTimeMillis() - startTime;

            startTime = System.currentTimeMillis();
            graphStored.addEdges(edges);
            graphStored.iterateAllEdges(edge -> {
                return null;
            });
            long storedTime = System.currentTimeMillis() - startTime;

            writer.println(iteration + " " + unionTime + " " + storedTime);
            System.out.println("Iteration: " + iteration);
        }

        writer.close();

    }


    /**
     *
     * @throws IOException
     */
    public static void benchmarkDVCInsertionsSimluated() throws IOException {
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
            generateEdges(edgesToAdd, bulkSize, edges);
            for (int i = 0; i < bulkSize; i++) {
                dvc.insertEdge(edges[i]);
            }
            added += bulkSize;

            long currentTime = System.currentTimeMillis();
            printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime);
            lastTime = currentTime;
        }

        writer.close();
    }

    private static int added = 0;
    private static long lastTime;

    public static void benchmarkDVCInsertionsReal() throws IOException {
        IGraph graph = new SimulatedGraph();
        IGraph graphToInsert = new ImmutableGraphWrapper(ImmutableGraph.load("testGraphs/it-2004"));
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        final int bulkSize =    10000000;
        PrintWriter writer = new PrintWriter("benchmarksdvc.data");

        added = 0;
        lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        graphToInsert.iterateAllEdges(edge -> {
            dvc.insertEdge(edge);
            added++;

            if(added % bulkSize == 0) {
                long currentTime = System.currentTimeMillis();
                printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime);
                lastTime = currentTime;
            }
            return null;
        });

        long currentTime = System.currentTimeMillis();
        printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime);
        writer.close();

        System.out.println("Graph size: " + graphToInsert.getNumberOfNodes());
        System.out.println("VC Dize: " + dvc.getVertexCoverSize());
    }


    public static void benchmarkDVCDeletionsSimulated() throws IOException {
        SimulatedGraph graph   = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        final long maxNode =        1382908;
        final int edgesToAdd =     16917053;
        final int bulkSize =          10000;

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
        System.out.print("DVC Size: " + dvc.getVertexCoverSize() + " ");
        System.out.println("Graph arcs: " + graph.getNumberOfArcs());

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
        //Benchmarks.benchmarkDVCInsertionsSimluated();
        Benchmarks.benchmarkDVCInsertionsReal();
        //Benchmarks.benchmarkDVCDeletions();
    }
}
