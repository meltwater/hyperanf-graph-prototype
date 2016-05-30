package it.unimi.dsi.big.webgraph.benchmarks;

import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraphWrapper;

import java.io.IOException;
import java.io.PrintWriter;

import static it.unimi.dsi.big.webgraph.benchmarks.BenchmarkUtils.*;

/**
 * Class for performing benchmarks that compares three different techniques for saving new edges.
 * Either
 *   1) Always store the graph to disk after every insertion
 *   2) Store the graph to disk when a certain memory overhead threashold has been reached
 *   3) Never store the graph
 *
 * Edges not saved to disk is kept in a high level data structure.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class UnionVsStoredGraph {

    private final String dateString = getDateString();
    private final String graphName = "in-2004";
    private final String graphFile = graphFolder + graphName;
    private final String dataFile = dataFolder + "unionVSStored" + dateString + ".data";

    private long maxNode = 10000000;
    private final int maxAddedEdges = 1000000;
    private int addedEdges = 0;

    private final int nrBenchmarkSamples = 20; /* Make sure that nrBenchmarkSamples * bulkSize * k = maxAddedEdges, for any integer k */
    private int bulkSize = 5000;

    private final float ratioThatNeverReaches = 20000000.0f;
    private final float ratioThatSometimesReaches = 8.0f;
    private final float ratioThatAlwaysReaches = 0.0f;

    private long storedTimeSinceSample         = 0;
    private long alwaysUnionTimeSinceSample    = 0;
    private long sometimesUnionTimeSinceSample = 0;

    private int iteration = 1;
private int iterationOfLastSample = 0;


    /**
     * Measures the memory usage and the time to iterate / add edges in the graph.
     *
     * @throws IOException
     */
    public void benchmark() throws IOException {
        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("%" + dateString + " " + graphName + "; " + bulkSize + "random edges inserted per iteration; The time measured is the time insert edges into the graph. ");
        writer.println("%Insertions bulkSize ratioBeforeSometimesSave nrNodes AlwaysUnionedTimems AlwaysUnionHeapSizeGB SometimesUnionedTimems SometimesUnionHeapSizeGB StoredTimedms StoredHeapSizeGB");

        ImmutableGraphWrapper graphAlwaysUnion    = new ImmutableGraphWrapper(ImmutableGraph.loadMapped(graphFile), ratioThatNeverReaches);
        ImmutableGraphWrapper graphSometimesUnion = new ImmutableGraphWrapper(ImmutableGraph.loadMapped(graphFile), ratioThatSometimesReaches);
        ImmutableGraphWrapper graphStored         = new ImmutableGraphWrapper(ImmutableGraph.loadMapped(graphFile), ratioThatAlwaysReaches);

        while (addedEdges < maxAddedEdges) {
            Edge[] edges = generateEdges(maxNode, bulkSize);

            storedTimeSinceSample          += benchmarkInsertEdges(edges, graphStored);
            alwaysUnionTimeSinceSample     += benchmarkInsertEdges(edges, graphAlwaysUnion);
            sometimesUnionTimeSinceSample  += benchmarkInsertEdges(edges, graphSometimesUnion);

            addedEdges += bulkSize;

            if(addedEdges % (maxAddedEdges / nrBenchmarkSamples) == 0 ) {
                printStoreVsUnionStatistics(writer, graphAlwaysUnion, graphSometimesUnion, graphStored);
                resetSampleTimes();
            }
            iteration++;
        }

        shutdownStoreVsUnionBenchmark(writer, graphAlwaysUnion, graphSometimesUnion, graphStored);
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


    private void printStoreVsUnionStatistics(PrintWriter writer, ImmutableGraphWrapper graphAlwaysUnion, ImmutableGraphWrapper graphSometimesUnion, ImmutableGraphWrapper graphStored) {
        float storedGraphSizeGigaBytes = graphStored.getMemoryUsageBytes() / (float) bytesPerGigaByte;
        float alwaysUnionGraphSizeGigaBytes = graphAlwaysUnion.getMemoryUsageBytes() / (float) bytesPerGigaByte;
        float sometimesUnionGraphSizeGigaBytes = graphSometimesUnion.getMemoryUsageBytes() / (float) bytesPerGigaByte;

        int numberOfIterationsBetweenSample = iteration - iterationOfLastSample;

        long averageTimeStoredSinceSample         = storedTimeSinceSample          / numberOfIterationsBetweenSample;
        long averageTimeAlwaysUnionSinceSample    = alwaysUnionTimeSinceSample     / numberOfIterationsBetweenSample;
        long averageTimeSometimesUnionSinceSample = sometimesUnionTimeSinceSample  / numberOfIterationsBetweenSample;

        writer.println(addedEdges / bulkSize + " " + bulkSize + " " + ratioThatSometimesReaches + " " + graphStored.numNodes() + " " +
                averageTimeAlwaysUnionSinceSample + " " + alwaysUnionGraphSizeGigaBytes + " " + averageTimeSometimesUnionSinceSample + " " + sometimesUnionGraphSizeGigaBytes + " " +
                averageTimeStoredSinceSample + " " + storedGraphSizeGigaBytes);
        writer.flush();
        System.out.println("Added edges: " + addedEdges);
    }

    private void resetSampleTimes() {
        storedTimeSinceSample         = 0;
        alwaysUnionTimeSinceSample    = 0;
        sometimesUnionTimeSinceSample = 0;
        iterationOfLastSample = iteration;
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


    public static void main(String[] args) {
        try {
            new UnionVsStoredGraph().benchmark();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
