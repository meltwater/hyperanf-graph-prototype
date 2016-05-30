package se.meltwater.benchmarks;

import it.unimi.dsi.big.webgraph.*;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static se.meltwater.benchmarks.BenchmarkUtils.*;

/**
 *
 * Class for performing different benchmarks of the Dynamic Vertex Cover.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class VertexCover {

    private static int added = 0;
    private static long lastTime;


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


        MutableGraph graphToInsert = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile));
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
    private static void insertAllEdgesFromGraphIntoDvc(int bulkSize, MutableGraph graphToInsert, DynamicVertexCover dvc, PrintWriter writer) {
        MutableGraph graph = new SimulatedGraph(); //Mock graph, DVC wont use it
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

        MutableGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));

        final long edgesToDelete = graph.numArcs();
        final int bulkSize =       10000000;

        System.out.println("Loading transpose");
        MutableGraph graphTranspose = graph.transpose();

        System.out.println("Inserting into DVC");
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + getDateString() + " " + graphName + "; " + edgesToDelete + " edges will be deleted from the dvc in bulks of " +
                bulkSize + ". The time measured is the time to delete " + bulkSize + " edges.");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        deleteAllEdgesFromDvcWithoutRemovingFromGraph(graph, edgesToDelete, bulkSize, graphTranspose, dvc, writer);

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
    private static void deleteAllEdgesFromDvcWithoutRemovingFromGraph(MutableGraph graph, long edgesToDelete, int bulkSize, MutableGraph graphTranspose, DynamicVertexCover dvc, PrintWriter writer) {
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

        final int edgesToRemove = (int)graph.numArcs();

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

        while(added < edgesToAdd) {
            Edge[] edges = generateEdges(maxNode, bulkSize);
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


}
