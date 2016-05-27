package se.meltwater.examples;


import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import se.meltwater.algo.*;
import se.meltwater.bfs.MSBreadthFirst;
import se.meltwater.bfs.StandardBreadthFirst;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.utils.Utils;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.graph.TraverseGraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

/**
 *
 * Class for performing a large variety of benchmarks.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class Benchmarks {

    private static float chanceNewNode = 1.0f;
    private static long bytesPerGigaByte = 1024 * 1024 * 1024;

    private static int added = 0;
    private static long lastTime;

    final static String graphFolder = "files/";
    final static String dataFolder = "files/";

    public static void benchmarkDanfVsHanf() throws IOException {

        final int log2m = 4;
        final int h = 3;
        int bulkSize = 30000;
        final long seed = 0L;

        final String graphName = "ljournal-2008";
        final String graphFile = graphFolder + graphName;

        Edge[] edges;

        edges = new Edge[bulkSize];
        int nrSamples = 10;

        long danfTotalTime = 0;
        long hBallTotalTime = 0;

        for (int i = 0; i < nrSamples; i++) {
            ImmutableGraphWrapper graph  = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile), 999999);
            DANF danf = new DANF(h, log2m, graph , seed);

            generateEdges(graph.getNumberOfNodes(), bulkSize, edges);

            System.out.println("Running iteration: " + i);
            System.out.println("Running DANF");
            long beforeDanf = System.currentTimeMillis();
            danf.addEdges(edges);
            long afterDanf = System.currentTimeMillis();
            danfTotalTime += afterDanf - beforeDanf;
            danf.close();
            danf = null;
            graph.close();
            graph = null;

            System.out.println("DANF current time: " + danfTotalTime / (i+1));


            ImmutableGraphWrapper graph2 = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile), 999999);
            System.out.println("Running HyperBoll");
            long beforeHBALL = System.currentTimeMillis();
            graph2.addEdges(edges);
            HyperBoll hyperBoll = new HyperBoll(graph2, log2m, seed);
            hyperBoll.init();
            for (int j = 1; j < h; j++) {
                hyperBoll.iterate();
            }
            hyperBoll.close();
            long afterHBALL = System.currentTimeMillis();
            hBallTotalTime += afterHBALL - beforeHBALL;
            graph2.close();
            graph2 = null;

            System.out.println("Currently, Danf: " + danfTotalTime / (i+1) + " HBall: " + hBallTotalTime / (i+1));
        }

        System.out.println("DANF Total: " + danfTotalTime / nrSamples + " ms, HBALL Total: " + hBallTotalTime / nrSamples + " ms.");

    }


    /**
     * Loads a real graph and performs both a standard bfs and a msbfs on it.
     * Their completion time is measured and saved into a data file.
     * The BFS's will originate from the first nodes in the graph.
     * @throws IOException
     * @throws InterruptedException
     */
    public static void benchmarkStdBfs() throws IOException, InterruptedException {
        final int maxNumberOfSources = 25;
        final int sourceBulkSize =4;
        final int startNode = 0;
        final int maxSteps = 8;
        final String dateString = getDateString();

        final String graphName = "noBlocksUk";
        final String graphFile = graphFolder + graphName;
        final String dataFile  = dataFolder + "benchmarkBfs" + dateString + ".data";

        ImmutableGraphWrapper graph = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + getDateString() + " " + graphName + "; Comparison between a standard implementation of BFS and the MS-BFS algorithm; "
                + " 0 to " + maxNumberOfSources + " sources are used for each bfs with " + sourceBulkSize + " bulk increase;" +
                " The time measured is the millis to perform a bfs that stops after h steps;");
        writer.println("#h nrSources stdbfsMillis msbfsMillis");

        int nrSources = sourceBulkSize;
        while(nrSources <= maxNumberOfSources) {
            long[] sources = LongStream.range(startNode, startNode + nrSources).toArray();

            for (int h = 1; h <= maxSteps; h++) {

                long stdTotalTime = performStandardBfsAndMeasureTime(sources, graph, h);
                long msbfsTotalTime = performMSBfsAndMeasureTime(sources, graph, h);

                System.out.println("Iteration " + h + " completed with " + nrSources + " sources.");
                writer.println(h + " " + sources.length + " " + stdTotalTime + " " + msbfsTotalTime);
                writer.flush();
            }

            nrSources += sourceBulkSize;
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
        StandardBreadthFirst bfs = new StandardBreadthFirst();
        long startTime = System.currentTimeMillis();
        bfs.breadthFirstSearch(sources, graph, h);
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    /**
     * Runs a ms-bfs from the sources in the current graph. The bfs will stop after h levels.
     * @param sources
     * @param graph
     * @param h
     * @return
     * @throws InterruptedException
     */
    private static long performMSBfsAndMeasureTime(long[] sources, IGraph graph, final int h) throws InterruptedException {

        MSBreadthFirst.Visitor visitor = (long node, BitSet bfsVisits, BitSet seen, int depth, MSBreadthFirst.Traveler t) -> {
            if(depth == h) {
                bfsVisits.clear();
            }
        };

        long startTime = System.currentTimeMillis();
        MSBreadthFirst msbfs = new MSBreadthFirst(graph);
        msbfs.breadthFirstSearch(sources,visitor);
        msbfs.close();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }


    /**
     * Benchmarks the difference between always storing the graph
     * to file vs bulking up unions of additional edges and storing them
     * after a certain threshold.
     * @throws IOException
     */
    public static void compareSimulatedAndTraverseGraph() throws FileNotFoundException {
        final int maxNumberOfEdges = 3000000;
        final int edgesBulkSize = 5000;
        int maxNode = maxNumberOfEdges;
        final int samples = 10;

        final String dateString = getDateString();
        final String dataFile = dataFolder + "benchmarkSimTrav" + dateString + ".data";

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("%" + getDateString() + "; Comparison between SimulatedGraph and TraverseGraph; " +
                edgesBulkSize + " are randomly generated and inserted into both. The time measured is the " +
                "time to insert the edges and perform a complete edge scan.");
        writer.println("%nrAddedEdges simulatedAddTimeMs simulatedIterateTimeMs simulatedMemoryGB traverseAddTimeMs traverseIterateTimeMs traverseMemoryGB");

        TraverseGraph tg = new TraverseGraph();
        long simulatedAddTotalTime     = 0;
        long simulatedIterateTotalTime = 0;

        SimulatedGraph sim = new SimulatedGraph();
        long traverseAddTotalTime     = 0;
        long traverseIterateTotalTime = 0;

        int nrAddedEdges = 0;
        while(nrAddedEdges < maxNumberOfEdges) {
            Edge[] edges = new Edge[edgesBulkSize];
            generateEdges(maxNode, edgesBulkSize, edges);

            long simulatedAddStartTime = System.currentTimeMillis();
            sim.addEdges(edges);
            simulatedAddTotalTime += System.currentTimeMillis() - simulatedAddStartTime;

            long simulatedIterateStartTime = System.currentTimeMillis();
            sim.iterateAllEdges(e -> null);
            simulatedIterateTotalTime += System.currentTimeMillis() - simulatedIterateStartTime;

            long traverseAddStartTime = System.currentTimeMillis();
            tg.addEdges(edges);
            traverseAddTotalTime += System.currentTimeMillis() - traverseAddStartTime;

            long traverseIterateStartTime = System.currentTimeMillis();
            iterateAllTraverseEdges(tg);
            traverseIterateTotalTime += System.currentTimeMillis() - traverseIterateStartTime;

            nrAddedEdges += edgesBulkSize;

            if(nrAddedEdges % (maxNumberOfEdges / samples) == 0) {
                writeSimulatedVsTraverseStatistics(writer, tg, simulatedAddTotalTime, simulatedIterateTotalTime, sim, traverseAddTotalTime, traverseIterateTotalTime, nrAddedEdges);
            }
        }

        writer.close();
    }

    private static void writeSimulatedVsTraverseStatistics(PrintWriter writer, TraverseGraph tg, long simulatedAddTotalTime, long simulatedIterateTotalTime, SimulatedGraph sim, long traverseAddTotalTime, long traverseIterateTotalTime, int nrAddedEdges) {
        float simulatedMemoryGB = Utils.getMemoryUsage(sim) / (float)bytesPerGigaByte;
        float traverseMemoryGB = Utils.getMemoryUsage(tg) / (float)bytesPerGigaByte;

        writer.println(nrAddedEdges + " " + simulatedAddTotalTime + " " + simulatedIterateTotalTime + " " + simulatedMemoryGB + " " + traverseAddTotalTime + " " + traverseIterateTotalTime + " " + traverseMemoryGB);
        writer.flush();
        System.out.println("Total nr edges: " + nrAddedEdges);
    }

    private static void iterateAllTraverseEdges(TraverseGraph tg) {
        int i = 0;
        NodeIterator it = tg.nodeIterator();
        while(it.hasNext()){
            it.nextLong();
            LazyLongIterator neighIt = it.successors();
            long neighbor;
            while((neighbor = neighIt.nextLong()) != -1) i++;
        }
    }

    /**
     * Performs a benchmark that compares three different techniques for saving new edges.
     * Either
     *   1) Always store the graph to disk after every insertion
     *   2) Store the graph to disk when a certain memory overhead threashold has been reached
     *   3) Never store the graph
     *
     * Edges not saved to disk is kept in a high level data structure.
     *
     * The benchmark measures the memory usage and the time to iterate / add edges in the graph.
     *
     * @throws IOException
     */
    public static void benchmarkUnionVsStored() throws IOException {
        final String dateString = getDateString();
        final String graphName = "in-2004";
        final String graphFile = graphFolder + graphName;
        final String dataFile = dataFolder + "unionVSStored" + dateString + ".data";

        long maxNode = 10000000;
        final int maxAddedEdges = 1000000;
        int addedEdges = 0;

        final int nrBenchmarkSamples = 20; /* Make sure that nrBenchmarkSamples * bulkSize * k = maxAddedEdges, for any integer k */
        int bulkSize = 5000;

        final float ratioThatNeverReaches = 20000000.0f;
        final float ratioThatSometimesReaches = 8.0f;
        final float ratioThatAlwaysReaches = 0.0f;

        PrintWriter writer = new PrintWriter(dataFile);

        writer.println("%" + dateString + " " + graphName + "; " + bulkSize + "random edges inserted per iteration; The time measured is the time insert edges into the graph. ");
        writer.println("%Insertions bulkSize ratioBeforeSometimesSave nrNodes AlwaysUnionedTimems AlwaysUnionHeapSizeGB SometimesUnionedTimems SometimesUnionHeapSizeGB StoredTimedms StoredHeapSizeGB");

        ImmutableGraphWrapper graphAlwaysUnion    = new ImmutableGraphWrapper(ImmutableGraph.loadMapped(graphFile), ratioThatNeverReaches);
        ImmutableGraphWrapper graphSometimesUnion = new ImmutableGraphWrapper(ImmutableGraph.loadMapped(graphFile), ratioThatSometimesReaches);
        ImmutableGraphWrapper graphStored         = new ImmutableGraphWrapper(ImmutableGraph.loadMapped(graphFile), ratioThatAlwaysReaches);

        long storedTimeSinceSample         = 0;
        long alwaysUnionTimeSinceSample    = 0;
        long sometimesUnionTimeSinceSample = 0;

        int iteration = 1;
        int iterationOfLastSample = 0;

        while (addedEdges < maxAddedEdges) {
            Edge[] edges = new Edge[bulkSize];
            generateEdges(maxNode, bulkSize, edges);

            storedTimeSinceSample          += benchmarkInsertEdges(edges, graphStored);
            alwaysUnionTimeSinceSample     += benchmarkInsertEdges(edges, graphAlwaysUnion);
            sometimesUnionTimeSinceSample  += benchmarkInsertEdges(edges, graphSometimesUnion);

            addedEdges += bulkSize;

            if(addedEdges % (maxAddedEdges / nrBenchmarkSamples) == 0 ) {
                printStoreVsUnionStatistics(addedEdges, bulkSize, ratioThatSometimesReaches, writer, graphAlwaysUnion, graphSometimesUnion, graphStored, storedTimeSinceSample, alwaysUnionTimeSinceSample, sometimesUnionTimeSinceSample, iteration, iterationOfLastSample);

                storedTimeSinceSample         = 0;
                alwaysUnionTimeSinceSample    = 0;
                sometimesUnionTimeSinceSample = 0;
                iterationOfLastSample = iteration;
            }
            iteration++;
        }

        shutdownStoreVsUnionBenchmark(writer, graphAlwaysUnion, graphSometimesUnion, graphStored);
    }


    private static void printStoreVsUnionStatistics(int addedEdges, int bulkSize, float ratioThatSometimesReaches, PrintWriter writer, ImmutableGraphWrapper graphAlwaysUnion, ImmutableGraphWrapper graphSometimesUnion, ImmutableGraphWrapper graphStored, long storedTimeSinceSample, long alwaysUnionTimeSinceSample, long sometimesUnionTimeSinceSample, int iteration, int iterationOfLastSample) {
        float storedGraphSizeGigaBytes = graphStored.getMemoryUsageBytes() / (float) bytesPerGigaByte;
        float alwaysUnionGraphSizeGigaBytes = graphAlwaysUnion.getMemoryUsageBytes() / (float) bytesPerGigaByte;
        float sometimesUnionGraphSizeGigaBytes = graphSometimesUnion.getMemoryUsageBytes() / (float) bytesPerGigaByte;

        int numberOfIterationsBetweenSample = iteration - iterationOfLastSample;

        long averageTimeStoredSinceSample         = storedTimeSinceSample          / numberOfIterationsBetweenSample;
        long averageTimeAlwaysUnionSinceSample    = alwaysUnionTimeSinceSample     / numberOfIterationsBetweenSample;
        long averageTimeSometimesUnionSinceSample = sometimesUnionTimeSinceSample  / numberOfIterationsBetweenSample;

        writer.println(addedEdges / bulkSize + " " + bulkSize + " " + ratioThatSometimesReaches + " " + graphStored.getNumberOfNodes() + " " +
                averageTimeAlwaysUnionSinceSample + " " + alwaysUnionGraphSizeGigaBytes + " " + averageTimeSometimesUnionSinceSample + " " + sometimesUnionGraphSizeGigaBytes + " " +
                averageTimeStoredSinceSample + " " + storedGraphSizeGigaBytes);
        writer.flush();
        System.out.println("Added edges: " + addedEdges);
    }

    /**
     * Closes all relevant classes from the StoreVsUnion benchmark
     */
    private static void shutdownStoreVsUnionBenchmark(PrintWriter writer, ImmutableGraphWrapper graphAlwaysUnion, ImmutableGraphWrapper graphSometimesUnion, ImmutableGraphWrapper graphStored) {
        graphSometimesUnion.close();
        graphAlwaysUnion.close();
        graphStored.close();
        writer.close();
    }

    /**
     * Inserts {@code edges} into {@code graph} and measures the time it takes.
     * @param edges The edges to insert
     * @param graph The graph to insert {@code edges} into
     * @return The time in milliseconds for insertions
     */
    private static long benchmarkInsertEdges(Edge[] edges, ImmutableGraphWrapper graph) {
        long startTime = System.currentTimeMillis();
        graph.addEdges(edges);
        graph.iterateAllEdges(edge -> {
           return null;
        });
        return System.currentTimeMillis() - startTime;
    }

    /**
     * Inserts random edges into a vertex cover and benchmarks the heap size and the time it takes to insert the edges.
     * @throws IOException
     */
    public static void benchmarkDVCInsertionsSimluated() throws IOException {
        final String dateString = getDateString();
        final String dataFile = dataFolder + "DvcInsertionsSimulated" + dateString + ".data";

        final int maxNode    =   10000000;
        final int edgesToAdd =  100000000;
        final int bulkSize   =    1000000;

        SimulatedGraph graph = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + dateString + " Simulated Graph;" + edgesToAdd + " edges are added in bulks of " + bulkSize + ";");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        insertRandomEdgesIntoDvc(graph, dvc, maxNode, edgesToAdd, bulkSize, false, writer);

        writer.close();
    }



    /**
     * Inserts all edges from a real graph into a vertex cover.
     * Measures the time to insert a bulk of edges.
     * @throws IOException
     */
    public static void benchmarkDVCInsertionsReal() throws IOException {
        final String dateString = getDateString();
        final String graphName = "noBlocksUk";
        final String graphFile = "testGraphs/" + graphName;
        final String dataFile = dataFolder + "DvcInsertionsReal" + dateString + ".data";

        final int bulkSize =    10000000;


        IGraph graphToInsert = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));
        DynamicVertexCover dvc = new DynamicVertexCover(new SimulatedGraph());

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + getDateString() + " " + graphName + "; All edges in " + graphName +
                " is inserted into an empty DVC. The time measured it the time to insert " + bulkSize + " edges.");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        insertAllEdgesFromGraphIntoDvc(bulkSize, graphToInsert, dvc, writer );

        writer.close();
    }

    /**
     * Inserts all edges in {@code graphToInsert} into {@code dvc} in bulk of {@code bulkSize}.
     * @param bulkSize
     * @param graphToInsert
     * @param dvc
     * @param writer
     */
    private static void insertAllEdgesFromGraphIntoDvc(int bulkSize, IGraph graphToInsert, DynamicVertexCover dvc, PrintWriter writer) {
        IGraph graph = new SimulatedGraph(); //Mock graph, DVC wont use it
        // These needs to be global for lambda to work
        added = 0;
        lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        graphToInsert.iterateAllEdges(edge -> {
            dvc.insertEdge(edge);
            added++;

            if(added % bulkSize == 0) {
                long currentTime = System.currentTimeMillis();
                printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph, dvc);
                lastTime = System.currentTimeMillis();
            }
            return null;
        });
    }

    /**
     * Inserts all edges from a graph into a vertex cover and
     * then deletes them sequentially from the vertex cover.
     * The time measured is the time to delete a bulk of edges.
     * @throws IOException
     */
    public static void benchmarkDVCDeletionsReal() throws IOException {
        final String dateString = getDateString();
        final String dataFile = dataFolder + "DvcDeletionsReal" + dateString + ".data";
        final String graphName = "it-2004";
        final String graphFile = graphFolder + graphName;

        IGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));

        final long edgesToDelete = graph.getNumberOfArcs();
        final int bulkSize =       10000000;

        System.out.println("Loading transpose");
        IGraph graphTranspose = graph.transpose();

        System.out.println("Inserting into DVC");
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + getDateString() + " " + graphName + "; " + edgesToDelete + " edges will be deleted from the dvc in bulks of " +
                bulkSize + ". The time measured is the time to delete " + bulkSize + " edges.");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        deleteAllEdgesFromDvc(graph, edgesToDelete, bulkSize, graphTranspose, dvc, writer);

        writer.close();
    }

    /**
     * Deletes all edges in {@code graph} from {@code dvc} in bulks of {@code bulkSize}.
     * @param graph
     * @param edgesToDelete
     * @param bulkSize
     * @param graphTranspose
     * @param dvc
     * @param writer
     */
    private static void deleteAllEdgesFromDvc(IGraph graph, long edgesToDelete, int bulkSize, IGraph graphTranspose, DynamicVertexCover dvc, PrintWriter writer) {
        // Global for lambda to work
        added = 0;
        System.out.println("Starting deletion");
        lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        graph.iterateAllEdges(edge -> {
           if(added++ == edgesToDelete) {
               return edge;
           }

            if(added % bulkSize == 0) {
                long currentTime = System.currentTimeMillis();
                printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph, dvc);
                lastTime = System.currentTimeMillis();
            }

            dvc.deleteEdge(edge, graphTranspose);

            return null;
        });
    }

    /**
     * Inserts random edges into a simulated graph and then deletes them.
     * The time measures is the time to delete a bulk of edges.
     * @throws IOException
     */
    public static void benchmarkDVCDeletionsSimulated() throws IOException {
        final String dateString = getDateString();
        final String dataFile = dataFolder + "DvcDeletionsSimulated" + dateString +".data";

        final long maxNode =    10000000;
        final int edgesToAdd =   1000000;
        final int bulkSize =         100;

        SimulatedGraph graph   = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        insertRandomEdgesIntoDvc(graph, dvc, maxNode, edgesToAdd, bulkSize, true, null);

        final int edgesToRemove = (int)graph.getNumberOfArcs();

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + dateString + " Simulated Graph; " + edgesToRemove + " edges will be deleted from the dvc in bulks of " +
                bulkSize + ". The time measured is the time to delete " + bulkSize + " edges.");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        deleteEdgesFromDvc(graph, dvc, edgesToRemove, bulkSize, writer, true);

        writer.close();
    }

    /**
     * Generates edges and inserts them into the dynamic vertex cover.
     * Optionally inserts the edges into {@code graph} as well.
     * @param graph The graph used
     * @param dvc The dynamic vertex cover to update
     * @param maxNode The maximum node id that can be generated
     * @param edgesToAdd The number of edges to add
     * @param bulkSize The edges will be added in bulks of {@code bulkSize}
     * @param shouldAddToGraph True if the edges should be inserted into {@code graph}
     * @param writer The file writer to print the statistics to
     */
    private static void insertRandomEdgesIntoDvc(SimulatedGraph graph, DynamicVertexCover dvc, long maxNode, int edgesToAdd, int bulkSize, boolean shouldAddToGraph, PrintWriter writer) {

        int added = 0;
        long startTime = System.currentTimeMillis();
        long lastTime = startTime;

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
                printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph, dvc);
            }
            lastTime = System.currentTimeMillis();
        }
    }

    /**
     * Deletes the first {@code edgesToRemove} edges in {@code graph} from {@code dvc} in bulks of {@code bulkSize}
     * @param graph The graph containing the edges to remove
     * @param dvc The vertex cover to remove edges from
     * @param edgesToRemove The number of edges to remove
     * @param bulkSize How many edges that should be removed before a time/heap measure
     * @param writer The file writer to print statistics to
     * @param shouldShuffle True if the edges should be deleted in a permutated order
     */
    private static void deleteEdgesFromDvc(SimulatedGraph graph, DynamicVertexCover dvc, int edgesToRemove, int bulkSize, PrintWriter writer, boolean shouldShuffle) {
        SimulatedGraph graphTranspose = (SimulatedGraph) graph.transpose();

        Edge[] edges = getEdges(graph, shouldShuffle);

        int removed = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        while(removed < edgesToRemove) {
            int nrDeleted = Math.min(bulkSize, edgesToRemove - removed);

            for (int i = 0; i < nrDeleted; i++) {
                Edge edgeToRemove = edges[i + removed];
                graph.deleteEdge(edgeToRemove);
                graphTranspose.deleteEdge(edgeToRemove.flip());
                dvc.deleteEdge(edgeToRemove, graphTranspose);
            }

            long currentTime = System.currentTimeMillis();
            printAndLogStatistics(writer, removed, lastTime, startTime, nrDeleted, currentTime, graph, dvc );

            lastTime = System.currentTimeMillis();
            removed += nrDeleted;
        }
    }

    /**
     * Returns the edges contained in {@code graph}.
     * @param graph The graph containing the edges
     * @param shouldShuffle True if the edges should be returned in shuffled order
     * @return
     */
    private static Edge[] getEdges(SimulatedGraph graph, boolean shouldShuffle) {
        ArrayList<Edge> edgeList = new ArrayList<>(Arrays.asList(graph.getAllEdges()));
        if(shouldShuffle) {
            Collections.shuffle(edgeList);
        }
        return edgeList.toArray(new Edge[edgeList.size()]);
    }

    /**
     * Benchmarks the difference between the DANF implementation
     * and the Trivial 2-bfs algorithm described in the publication
     * DANF: Approximate Neighborhood Function on Large Dynamic Graphs.
     * @throws IOException
     * @throws InterruptedException
     */
    public static void benchmarkDANFToTrivial() throws IOException, InterruptedException {
        final String dateString = getDateString();
        final String dataFile = dataFolder + "DANFComparedToTrivial" + dateString + ".data";
        final String graphName = "in-2004";
        final String graphFile = graphFolder + graphName;

        final int log2m = 7;
        final int h = 3;
        final int maxBulkSize = 6400;
        int bulkSize =      640;
        final int bulkSizeIncrease = 640;

        IGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));
        IGraph graph2 = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));

        DANF danf = new DANF(h,log2m,graph);
        TrivialDynamicANF tanf = new TrivialDynamicANF(h,log2m,graph2);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("%" + dateString + " " + graphName + " " + maxBulkSize + " edges will be inserted into DANF in bulks of " +
                bulkSize + ". The time measured is the time to insert " + bulkSize + " edges.; h is set to " + h + " and log2m is " + log2m + ";");
        writer.println("%BulkSize DanfEPS DanfGraphMemory DanfCounterMemory DanfVcMemory DanfMsbfsMemory TrivialEPS TrivialMemory ElapsedTime nrArcs nrNodes");

        int  added = 0;
        long totalDanfTime = 0;
        long totalTrivialTime = 0;
        long start = System.currentTimeMillis();

        Edge[] edges;

        System.out.println("Starting edge insertions");
        while(bulkSize <= maxBulkSize) {
            edges = new Edge[bulkSize];
            generateEdges(graph.getNumberOfNodes(), bulkSize, edges);

            long beforeDanf = System.currentTimeMillis();
            danf.addEdges(edges);
            long afterDanf = System.currentTimeMillis();
            totalDanfTime += afterDanf-beforeDanf;

            long beforeTrivial = System.currentTimeMillis();
            tanf.addEdges(edges);
            long afterTrivial = System.currentTimeMillis();
            totalTrivialTime += afterTrivial-beforeTrivial;
            added += bulkSize;

            float danfGraphGB = danf.getMemoryUsageGraphBytes()/(float)bytesPerGigaByte ;
            float danfCounterGB = danf.getMemoryUsageCounterBytes()/(float)bytesPerGigaByte;
            float danfVCGB = danf.getMemoryUsageVCBytes() / (float) bytesPerGigaByte;
            float danfMSBFSGB = danf.getMemoryUsageMsBfsBytes() / (float) bytesPerGigaByte;
            float danfTotalMemory = danfGraphGB + danfCounterGB + danfVCGB + danfMSBFSGB;

            float danfEps = (float) added / totalDanfTime * 1000;
            float trivialEps = (float) added / totalTrivialTime * 1000;
            float trivialTotalMemory = tanf.getMemoryUsageBytes()/(float)bytesPerGigaByte;

            long totalTimeSeconds = (afterTrivial - start) / 1000;

            writer.println(bulkSize + " " + danfEps + " " + danfGraphGB+ " " + danfCounterGB + " " + danfVCGB + " " + danfMSBFSGB
                    + " " + trivialEps + " " + trivialTotalMemory + " " + (afterTrivial-start) + " " + added + " " + graph.getNumberOfNodes());
            writer.flush();

            System.out.println(bulkSize + " edges, " + danfEps + " Danf DPS, " + danfTotalMemory + "GB DANF memory, " + trivialEps + " Trivial DPS, " +
                     trivialTotalMemory + "GB trivial memory, " + totalTimeSeconds + "s total, " + added + " " + graph.getNumberOfNodes());

            bulkSize += bulkSizeIncrease;
        }

        danf.close();
        writer.close();
    }

    /**
     * Benchmarks the time to insert edges into Dvanf using a real graph.
     * @throws IOException
     * @throws InterruptedException
     */
    public static void benchmarkEdgeInsertionsDanfReal() throws IOException, InterruptedException {
        final String dateString = getDateString();
        final String dataFile = dataFolder + "EdgeInsertionsDanfReal" + dateString + ".data";
        final String graphName = "noBlocksUk";
        final String graphFile = graphFolder + graphName;

        final int log2m = 7;
        final int h = 3;
        final int edgesToAdd =   1000000;
        final int bulkSize =      10000;

        System.out.println("Loading graph");
        IGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));

        DANF danf = new DANF(h,log2m,graph);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + dateString + " " + graphName + " " + edgesToAdd + " edges will be inserted into DANF in bulks of " +
                bulkSize + ". The time measured is the time to insert " + bulkSize + " edges.; h is set to " + h + " and log2m is " + log2m + ";");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        insertEdgesIntoDanf(edgesToAdd, bulkSize, graph, danf, writer);

        writer.close();
    }

    /**
     * Inserts {@code edgesToAdd} random edges into {@code danf}
     * @param edgesToAdd The number of edges to add
     * @param bulkSize The number of edges to add in a bulk
     * @param graph The graph in danf
     * @param danf
     * @param writer
     * @throws IOException
     * @throws InterruptedException
     */
    private static void insertEdgesIntoDanf(int edgesToAdd, int bulkSize, IGraph graph, DynamicNeighborhoodFunction danf, PrintWriter writer) throws IOException, InterruptedException {
        int  added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        Edge[] edges = new Edge[bulkSize];

        System.out.println("Starting edge insertions");
        while(System.in.available() == 0 && added < edgesToAdd) {
            generateEdges(graph.getNumberOfNodes(), bulkSize, edges);

            danf.addEdges(edges);
            added += bulkSize;

            long currentTime = System.currentTimeMillis();

            printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph, danf);
            lastTime = System.currentTimeMillis();
        }

        danf.close();
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
    private static void printAndLogStatistics(PrintWriter writer, int nrModified, long lastTime, long startTime, int bulkSize, long currentTime, IGraph graph, Object objectToMeasureMemory) {
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
        System.out.print("Heap size: " + heapSize + " Gb. ");
        System.out.print("NrArcs: " + 0/*graph.getNumberOfArcs()*/ + " ");
        System.out.print("NrNodes: " + graph.getNumberOfNodes() + " ");
        System.out.println("Total time: " + elapsedTimeSinceStart + "s.");

        writer.println(modifiedInMillions + " " + dps + " " + heapSize + " " + elapsedTimeSinceStart  + " " + /*graph.getNumberOfArcs()*/0 + " " + graph.getNumberOfNodes());
        writer.flush();
    }

    /**
     * Generates a bulk of random edges. {@code edges} must have {@code bulkSize} memory allocated
     * @param currentMaxNode
     * @param bulkSize
     * @param edges
     */
    private static void generateEdges(long currentMaxNode, int bulkSize, Edge[] edges) {
        long maxNewNodes = (long)(currentMaxNode * chanceNewNode);

        for (int i = 0; i < bulkSize; i++) {
            long from = ThreadLocalRandom.current().nextLong(maxNewNodes );
            long to = ThreadLocalRandom.current().nextLong(maxNewNodes );
            edges[i] = new Edge(from, to);
        }
    }

    /**
     * Returns the current time as date
     * @return
     */
    private static String getDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //Benchmarks.benchmarkEdgeInsertionsDanfReal();
        //benchmarkDANFToTrivial();
        Benchmarks.benchmarkDanfVsHanf();
        //Benchmarks.benchmarkDVCInsertionsSimluated();
        //Benchmarks.benchmarkDVCInsertionsReal();
        //Benchmarks.benchmarkDVCDeletionsSimulated();
        //Benchmarks.benchmarkDVCDeletionsReal();
        //Benchmarks.benchmarkUnionVsStored();
        //Benchmarks.benchmarkStdBfs();
        //Benchmarks.compareSimulatedAndTraverseGraph();
    }
}


