package se.meltwater.examples;


import com.google.common.collect.Range;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.big.webgraph.Transform;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.bfs.MSBreadthFirst;
import se.meltwater.bfs.StandardBreadthFirst;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.utils.Utils;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Remove copy-paste!
 */
public class Benchmarks {

    private static float chanceNewNode = 1.01f;
    private static long bytesPerGigaByte = 1024 * 1024 * 1024;

    private static int added = 0;
    private static long lastTime;


    /**
     * Loads a real graph and performs both a standard bfs and a msbfs on it.
     * Their completion time is measured and saved into a data file.
     * The BFS's will originate from the first nodes in the graph.
     * @throws IOException
     * @throws InterruptedException
     */
    public static void benchmarkStdBfs() throws IOException, InterruptedException {
        final int numberOfSources = 5000;
        final int startNode = 5000;
        final int maxSteps = 10;
        final String graphFile = "testGraphs/in-2004";
        final String dataFile = "benchmarkBfs.data";

        ImmutableGraphWrapper graph = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));
        long[] sources = LongStream.range(startNode, startNode + numberOfSources).toArray();

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#h nrSources stdbfsMillis msbfsMillis");

        for (int h = 0; h <= maxSteps; h++) {
            long stdTotalTime = performStandardBfsAndMeasureTime(sources, graph, h);
            long msbfsTotalTime = performMSBfsAndMeasureTime(sources, graph, h);

            System.out.println("Iteration " + h + " completed.");
            writer.println(h + " " + sources.length + " " + stdTotalTime + " " + msbfsTotalTime);
        }

        writer.close();
    }

    /**
     * Runs a std bfs from the sources in the current graph. The bfs will stop after h levels.
     * @param sources
     * @param graph
     * @param h
     * @return The elapsed time in millis
     */
    private static long performStandardBfsAndMeasureTime(long[] sources, IGraph graph, int h) {
        long startTime = System.currentTimeMillis();
        StandardBreadthFirst.breadthFirstSearch(sources, graph, h);
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    /**
     *
     * @param sources
     * @param graph
     * @param h
     * @return
     * @throws InterruptedException
     */
    private static long performMSBfsAndMeasureTime(long[] sources, IGraph graph, final int h) throws InterruptedException {
        //Dummy traveler
        MSBreadthFirst.Traveler traveler = (MSBreadthFirst.Traveler t1, int depth) ->  t1;
        MSBreadthFirst.Traveler[] travelers = Utils.repeat(traveler,sources.length, new MSBreadthFirst.Traveler[0]);

        MSBreadthFirst.Visitor visitor = (long node, BitSet bfsVisits, BitSet seen, int depth, MSBreadthFirst.Traveler t) -> {
            if(depth == h) {
                bfsVisits.clear();
            }
        };

        long startTime = System.currentTimeMillis();
        MSBreadthFirst msbfs = new MSBreadthFirst(sources, travelers ,graph,visitor);
        msbfs.breadthFirstSearch();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }





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
        int bulkSize =      5000;

        //SimulatedGraph graph = new SimulatedGraph();
        //graph.addNode(0);

        System.out.println("Loading graph");
        IGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped("testGraphs/it-2004"));

        DANF danf = runHyperBoll(log2m, h, graph);

        PrintWriter writer = getInititatedPrinter("benchmarksSimon.data");

        int added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        Edge[] edges = new Edge[bulkSize];

        System.out.println("Starting edge insertions");
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

    private static DANF runHyperBoll(int log2m, int h, IGraph graph) throws IOException {
        System.out.println("Calculating VC");
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        System.out.println("Calculating Transpose");
        DANF danf = new DANF(dvc, h, graph);

        System.out.println("Starting HyperBall");
        HyperBoll hyperBoll = new HyperBoll(graph, log2m);
        hyperBoll.init();
        for (int i = 1; i <= h; i++) {
            System.out.println("HyperBoll iteration: " + i);
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
        //Benchmarks.benchmarkDVCDeletionsReal();
        //Benchmarks.benchmarkUnionVsStored();
        Benchmarks.benchmarkStdBfs();
    }
}
