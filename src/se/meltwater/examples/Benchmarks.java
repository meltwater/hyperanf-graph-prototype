package se.meltwater.examples;


import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
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
import se.meltwater.graph.TraverseGraph;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
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

    final static String graphFolder = "files/";
    final static String dataFolder = "files/";



    /**
     * Loads a real graph and performs both a standard bfs and a msbfs on it.
     * Their completion time is measured and saved into a data file.
     * The BFS's will originate from the first nodes in the graph.
     * @throws IOException
     * @throws InterruptedException
     */
    public static void benchmarkStdBfs() throws IOException, InterruptedException {
        final int maxNumberOfSources = 5000;
        final int sourceBulkSize = 500;
        final int startNode = 0;
        final int maxSteps = 8;
        final String dateString = getDateString();

        final String graphName = "in-2004";
        final String graphFile = graphFolder + graphName;
        final String dataFile = dataFolder + "benchmarkBfs" + dateString + ".data";

        ImmutableGraphWrapper graph = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));


        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + getDateString() + " " + graphName + "; Comparison between a standard implementation of BFS and the MS-BFS algorithm; "
                + " 0 to " + maxNumberOfSources + " sources are used for each bfs with " + sourceBulkSize + " bulk increase;" +
                " The time measured is the millis to perform a bfs that stops after h steps;");
        writer.println("#h nrSources stdbfsMillis msbfsMillis");

        int nrSources = 0;
        while(nrSources <= maxNumberOfSources) {
            long[] sources = LongStream.range(startNode, startNode + nrSources).toArray();

            for (int h = 0; h <= maxSteps; h++) {
                long stdTotalTime = performStandardBfsAndMeasureTime(sources, graph, h);
                long msbfsTotalTime = performMSBfsAndMeasureTime(sources, graph, h);

                System.out.println("Iteration " + h + " completed with " + nrSources + " sources.");
                writer.println(h + " " + sources.length + " " + stdTotalTime + " " + msbfsTotalTime);
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

        MSBreadthFirst.Visitor visitor = (long node, BitSet bfsVisits, BitSet seen, int depth, MSBreadthFirst.Traveler t) -> {
            if(depth == h) {
                bfsVisits.clear();
            }
        };

        long startTime = System.currentTimeMillis();
        MSBreadthFirst msbfs = new MSBreadthFirst(sources, graph,visitor);
        msbfs.breadthFirstSearch();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }





    public static void compareSimulatedAndTraverseGraph() throws FileNotFoundException {
        final int maxNumberOfEdges = 1000000;
        final int sourceBulkSize = 50000;
        final int startNode = 0;
        final int maxSteps = 8;
        final String dateString = getDateString();

        final String dataFile = dataFolder + "benchmarkSimTrav" + dateString + ".data";


        /*PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + getDateString() + "; Comparison between SimulatedGraph and TraverseGraph; ");
        writer.println("#h nrSources stdbfsMillis msbfsMillis");*/

        int nrEdges = sourceBulkSize;
        TraverseGraph tg;
        SimulatedGraph sim;
        while(nrEdges <= maxNumberOfEdges) {
            Edge[] edges = new Edge[nrEdges];
            generateEdges(nrEdges, nrEdges, edges);

            long time = System.currentTimeMillis();
            sim = new SimulatedGraph();
            sim.addEdges(edges);
            sim.iterateAllEdges((Edge e) -> null);
            System.out.println("Added " + nrEdges + " edges and iterated to SimulatedGraph in " + (System.currentTimeMillis()-time) + "ms.");

            time = System.currentTimeMillis();
            tg = new TraverseGraph(edges);
            int i = 0;
            NodeIterator it = tg.nodeIterator();
            while(it.hasNext()){
                it.nextLong();
                LazyLongIterator neighIt = it.successors();
                long neighbor;
                while((neighbor = neighIt.nextLong()) != -1) i++;
            }
            System.out.println("Added " + nrEdges + " edges and iterated to TraverseGraph in " + (System.currentTimeMillis()-time) + "ms." + i);

            System.out.println("");
            //writer.println(h + " " + sources.length + " " + stdTotalTime + " " + msbfsTotalTime);


            nrEdges += sourceBulkSize;
        }

        //writer.close();
    }

    public static void benchmarkUnionVsStored() throws IOException {
        final String dateString = getDateString();
        final String graphName = "in-2004";
        final String graphFile = graphFolder + graphName;
        final String dataFile = dataFolder + "unionVSStored" + dateString + ".data";

        ImmutableGraphWrapper graphUnioned = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));
        ImmutableGraphWrapper graphStored = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));

        long maxNode = 1000000;
        int bulkSize = 1000;

        Edge[] edges = new Edge[bulkSize];
        int iteration = 0;
        int maxIteration = 50;
        PrintWriter writer = new PrintWriter(dataFile);

        writer.println("#" + dateString + " " + graphName + "; " + bulkSize + " random edges inserted per iteration; Time measured to insert edges and to perform a complete edge scan. ");
        writer.println("#Iteration UnionedTimems StoredTimedms");

        while (iteration++ < maxIteration) {
            generateEdges(maxNode, bulkSize, edges);

            long startTime = System.currentTimeMillis();
            graphUnioned.addEdgesUnioned(edges);
            graphUnioned.iterateAllEdges(edge -> null);

            long unionTime = System.currentTimeMillis() - startTime;

            startTime = System.currentTimeMillis();
            graphStored.addEdges(edges);
            graphStored.iterateAllEdges(edge -> null);
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
        final String dateString = getDateString();
        final String dataFile = dataFolder + "DvcInsertionsSimulated" + dateString + ".data";

        final int maxNode    =   10000000;
        final int edgesToAdd =  100000000;
        final int bulkSize   =    1000000;

        SimulatedGraph graph = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + dateString + " Simulated Graph;" + edgesToAdd + " edges are added in bulks of " + bulkSize + ";");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        insertRandomEdgesIntoDvc(graph, dvc, maxNode, edgesToAdd, bulkSize, lastTime, startTime, false, writer);

        writer.close();
    }



    /**
     *
     * @throws IOException
     */
    public static void benchmarkDVCInsertionsReal() throws IOException {
        final String dateString = getDateString();
        final String graphName = "it-2004";
        final String graphFile = "testGraphs/" + graphName;
        final String dataFile = dataFolder + "DvcInsertionsReal" + dateString + ".data";

        final int bulkSize =    10000000;

        IGraph graph = new SimulatedGraph(); //Mock graph, DVC wont use it
        IGraph graphToInsert = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + getDateString() + " " + graphName + "; All edges in " + graphName +
                " is inserted into an empty DVC. The time measured it the time to insert " + bulkSize + " edges.");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        // These needs to be global for lambda to work
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
    }

    /**
     *
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
        final String dateString = getDateString();
        final String dataFile = dataFolder + "DvcDeletionsSimulated" + dateString +".data";

        final long maxNode =    10000000;
        final int edgesToAdd =   1000000;
        final int bulkSize =         100;

        SimulatedGraph graph   = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        insertRandomEdgesIntoDvc(graph, dvc, maxNode, edgesToAdd, bulkSize, 0, 0, true, null);

        int removed = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;
        final int edgesToRemove = (int)graph.getNumberOfArcs();

        SimulatedGraph graphTranspose = (SimulatedGraph) graph.transpose();

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + dateString + " Simulated Graph; " + edgesToRemove + " edges will be deleted from the dvc in bulks of " +
                bulkSize + ". The time measured is the time to delete " + bulkSize + " edges.");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        deleteEdges(graph, graphTranspose, dvc, edgesToRemove, bulkSize, writer, removed, lastTime, startTime, true);

        writer.close();
    }

    /**
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public static void benchmarkEdgeInsertionsDanfReal() throws IOException, InterruptedException {
        final String dateString = getDateString();
        final String dataFile = dataFolder + "EdgeInsertionsDanfReal" + dateString + ".data";
        final String graphName = "in-2004";
        final String graphFile = graphFolder + graphName;

        final int log2m = 7;
        final int h = 3;
        final int edgesToAdd =   100000;
        final int bulkSize =      10000;

        System.out.println("Loading graph");
        IGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));

        DANF danf = runHyperBoll(log2m, h, graph);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + dateString + " " + graphName + " " + edgesToAdd + " edges will be inserted into DANF in bulks of " +
                bulkSize + ". The time measured is the time to insert " + bulkSize + " edges.; h is set to " + h + " and log2m is " + log2m + ";");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

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

    private static String getDateString() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
        Date date = new Date();
        return dateFormat.format(date);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Benchmarks.benchmarkEdgeInsertionsDanfReal();
        //Benchmarks.benchmarkDVCInsertionsSimluated();
        //Benchmarks.benchmarkDVCInsertionsReal();
        //Benchmarks.benchmarkDVCDeletionsSimulated();
        //Benchmarks.benchmarkDVCDeletionsReal();
        //Benchmarks.benchmarkUnionVsStored();
        //Benchmarks.benchmarkStdBfs();
        //Benchmarks.compareSimulatedAndTraverseGraph();
    }
}
