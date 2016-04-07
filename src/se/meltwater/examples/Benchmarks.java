package se.meltwater.examples;


import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.Transform;
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

        final int maxNode = 1000000;
        final int edgesToAdd = 100000000;
        final int bulkSize =     1000000;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        PrintWriter writer = getInititatedPrinter("benchmarksdvc.data");

        insertRandomEdgesIntoDvc(graph, dvc, maxNode, edgesToAdd, bulkSize, lastTime, startTime, false, writer);

        writer.close();
    }



    /**
     *
     * @throws IOException
     */
    public static void benchmarkDVCInsertionsReal() throws IOException {
        IGraph graph = new SimulatedGraph();
        IGraph graphToInsert = new ImmutableGraphWrapper(ImmutableGraph.load("testGraphs/indochina-2004"));
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        final int bulkSize =    10000000;
        PrintWriter writer = getInititatedPrinter("benchmarksdvc.data");

        added = 0;
        lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        graphToInsert.iterateAllEdges(edge -> {
            dvc.insertEdge(edge);
            added++;

            if(added % bulkSize == 0) {
                long currentTime = System.currentTimeMillis();
                printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph);
                lastTime = currentTime;
            }
            return null;
        });

        long currentTime = System.currentTimeMillis();
        printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph);
        writer.close();

        System.out.println("Graph size: " + graphToInsert.getNumberOfNodes());
        System.out.println("VC Dize: " + dvc.getVertexCoverSize());
    }

    /**
     *
     * @throws IOException
     */
    public static void benchmarkDVCDeletionsReal() throws IOException {
        IGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped("testGraphs/it-2004"));
        System.out.println("Graph loaded. Starting loading transpose");
        IGraph graphTranspose = graph.transpose();

        System.out.println("Transpose done");
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        PrintWriter writer = getInititatedPrinter("benchmarksDvcDeletionsReal.data");
        final long edgesToDelete = graph.getNumberOfArcs();
        final int bulkSize =          10000000;
        added = 0;
        lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        System.out.println("Starting deletion");
        graph.iterateAllEdges(edge -> {
           if(added++ == edgesToDelete) {
               return edge;
           }

            if(added % bulkSize == 0) {
                long currentTime = System.currentTimeMillis();
                printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph);
                lastTime = currentTime;
            }

            dvc.deleteEdge(edge, graphTranspose);

            return null;
        });

        writer.close();
    }

    /**
     *
     * @throws IOException
     */
    public static void benchmarkDVCDeletionsSimulated() throws IOException {
        SimulatedGraph graph   = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        final long maxNode =    10000000;
        final int edgesToAdd =   1000000;
        final int bulkSize =         100;

        insertRandomEdgesIntoDvc(graph, dvc, maxNode, edgesToAdd, bulkSize, 0, 0, true, null);

        PrintWriter writer = getInititatedPrinter("benchmarksdvcdelete.data");

        int removed = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;
        final int edgesToRemove = (int)graph.getNumberOfArcs();

        SimulatedGraph graphTranspose = (SimulatedGraph) graph.transpose();

        deleteEdges(graph, graphTranspose, dvc, edgesToRemove, bulkSize, writer, removed, lastTime, startTime, true);

        writer.close();
    }

    /**
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public static void benchmarkEdgeInsertionsDanf() throws IOException, InterruptedException {
        int log2m = 7;
        int h = 3;
        int edgesToAdd = 5000000;
        int bulkSize = 50000;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);

        DANF danf = runHyperBoll(log2m, h, graph);

        PrintWriter writer = getInititatedPrinter("benchmarksSimon.data");

        int added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        Edge[] edges = new Edge[bulkSize];

        while(System.in.available() == 0 && added < edgesToAdd) {
            generateEdges(graph.getNumberOfNodes(), bulkSize, edges);

            danf.addEdges(edges);
            added += bulkSize;

            long currentTime = System.currentTimeMillis();
            printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph);
            lastTime = currentTime;
        }

        writer.close();
    }

    private static void insertRandomEdgesIntoDvc(SimulatedGraph graph, DynamicVertexCover dvc, long maxNode, int edgesToAdd, int bulkSize, long lastTime, long startTime, boolean shouldAddToGraph, PrintWriter writer) {
        int added = 0;
        Edge[] edges = new Edge[bulkSize];
        while(added < edgesToAdd) {
            generateEdges(maxNode, bulkSize, edges);
            for (int i = 0; i < bulkSize; i++) {
                dvc.insertEdge(edges[i]);
            }

            if(shouldAddToGraph) {
                graph.addEdges(edges);
            }
            added += bulkSize;

            long currentTime = System.currentTimeMillis();
            if(writer != null) {
                printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph);
            }
            lastTime = currentTime;
        }
    }


    private static void deleteEdges(SimulatedGraph graph, SimulatedGraph graphTranspose, DynamicVertexCover dvc, int edgesToRemove, int bulkSize, PrintWriter writer, int removed, long lastTime, long startTime, boolean shouldShuffle) {
        ArrayList<Edge> edgeList = new ArrayList<>(edgesToRemove);
        graph.iterateAllEdges(edge -> {
            edgeList.add(edge);
            return null;
        });
        if(shouldShuffle) {
            Collections.shuffle(edgeList);
        }
        Edge[] edges = edgeList.toArray(new Edge[edgeList.size()]);

        while(removed < edgesToRemove) {
            int nrDeleted = Math.min(bulkSize, edgesToRemove - removed);

            for (int i = 0; i < nrDeleted; i++) {
                Edge edgeToRemove = edges[i + removed];
                graph.deleteEdge(edgeToRemove);
                graphTranspose.deleteEdge(edgeToRemove.flip());
                dvc.deleteEdge(edgeToRemove, graphTranspose);
            }

            long currentTime = System.currentTimeMillis();
            printAndLogStatistics(writer, removed, lastTime, startTime, nrDeleted, currentTime, graph );

            lastTime = currentTime;
            removed += nrDeleted;
        }
    }

    private static PrintWriter getInititatedPrinter(String fileName) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(fileName);
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");
        return writer;
    }

    private static void printAndLogStatistics(PrintWriter writer, int nrModified, long lastTime, long startTime, int bulkSize, long currentTime, IGraph graph) {
        long elapsedTime = currentTime - lastTime;
        float timePerBulkSeconds = elapsedTime / 1000.0f;
        float dps = bulkSize / timePerBulkSeconds;

        float heapSize = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (float)bytesPerGigaByte;

        float elapsedTimeSinceStart = (currentTime - startTime) / 1000.0f;
        float modifiedInMillions = (float)nrModified / 1000000;

        System.out.print("Total nr edges: " + nrModified + ". ");
        System.out.print("Modified " + bulkSize + " edges. ");
        System.out.print("Time per bulk: " + timePerBulkSeconds + "s. ");
        System.out.print("DPS: " + dps + ". ");
        System.out.print("Heap size: " + heapSize + " Gb. ");
        System.out.print("NrArcs: " + graph.getNumberOfArcs() + " ");
        System.out.print("NrNodes: " + graph.getNumberOfNodes() + " ");
        System.out.println("Total time: " + elapsedTimeSinceStart + "s.");

        writer.println(modifiedInMillions + " " + dps + " " + heapSize + " " + elapsedTimeSinceStart  + " " + graph.getNumberOfArcs() + " " + graph.getNumberOfNodes());
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
        //Benchmarks.benchmarkEdgeInsertionsDanf();
        //Benchmarks.benchmarkDVCInsertionsSimluated();
        //Benchmarks.benchmarkDVCInsertionsReal();
        //Benchmarks.benchmarkDVCDeletionsSimulated();
        Benchmarks.benchmarkDVCDeletionsReal();
        //Benchmarks.benchmarkUnionVsStored();
    }
}
