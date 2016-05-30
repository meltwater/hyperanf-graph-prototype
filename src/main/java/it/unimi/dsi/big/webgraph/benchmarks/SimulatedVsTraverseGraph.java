package it.unimi.dsi.big.webgraph.benchmarks;

import it.unimi.dsi.big.webgraph.*;
import it.unimi.dsi.big.webgraph.Utils;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import static it.unimi.dsi.big.webgraph.benchmarks.BenchmarkUtils.*;

/**
 *
 * Class for comparing the performance of {@link SimulatedGraph} and {@link TraverseGraph}.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class SimulatedVsTraverseGraph {

    private final int maxNumberOfEdges = 3000000;
    private final int edgesBulkSize = 5000;
    private int maxNode = maxNumberOfEdges;
    private final int samples = 10;

    private TraverseGraph tg = new TraverseGraph();
    private long simulatedAddTotalTime     = 0;
    private long simulatedIterateTotalTime = 0;

    private SimulatedGraph sim = new SimulatedGraph();
    private long traverseAddTotalTime     = 0;
    private long traverseIterateTotalTime = 0;

    private int nrAddedEdges = 0;

    private final String dateString = getDateString();
    private final String dataFile = dataFolder + "benchmarkSimTrav" + dateString + ".data";

    /**
     * Benchmarks the difference between always storing the graph
     * to file vs bulking up unions of additional edges and storing them
     * after a certain threshold.
     * @throws FileNotFoundException
     */
    public void benchmark() throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("%" + getDateString() + "; Comparison between SimulatedGraph and TraverseGraph; " +
                edgesBulkSize + " are randomly generated and inserted into both. The time measured is the " +
                "time to insert the edges and perform a complete edge scan.");
        writer.println("%nrAddedEdges simulatedAddTimeMs simulatedIterateTimeMs simulatedMemoryGB traverseAddTimeMs traverseIterateTimeMs traverseMemoryGB");

        while(nrAddedEdges < maxNumberOfEdges) {
            Edge[] edges = generateEdges(maxNode, edgesBulkSize);

            addAndIterateEdgesSimulated(edges);
            addAndIterateEdgesTraverse(edges);

            nrAddedEdges += edgesBulkSize;

            if(nrAddedEdges % (maxNumberOfEdges / samples) == 0) {
                writeSimulatedVsTraverseStatistics(writer, tg, simulatedAddTotalTime, simulatedIterateTotalTime, sim, traverseAddTotalTime, traverseIterateTotalTime, nrAddedEdges);
            }
        }
        writer.close();
    }

    private void addAndIterateEdgesTraverse(Edge[] edges) {
        long traverseAddStartTime = System.currentTimeMillis();
        tg.addEdges(edges);
        traverseAddTotalTime += System.currentTimeMillis() - traverseAddStartTime;

        long traverseIterateStartTime = System.currentTimeMillis();
        iterateAllTraverseEdges(tg);
        traverseIterateTotalTime += System.currentTimeMillis() - traverseIterateStartTime;
    }

    private void addAndIterateEdgesSimulated(Edge[] edges) {
        long simulatedAddStartTime = System.currentTimeMillis();
        sim.addEdges(edges);
        simulatedAddTotalTime += System.currentTimeMillis() - simulatedAddStartTime;

        long simulatedIterateStartTime = System.currentTimeMillis();
        sim.iterateAllEdges(e -> null);
        simulatedIterateTotalTime += System.currentTimeMillis() - simulatedIterateStartTime;
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

}
