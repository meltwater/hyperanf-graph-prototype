package it.unimi.dsi.big.webgraph.benchmarks;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraphWrapper;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.big.webgraph.algo.MSBreadthFirst;
import it.unimi.dsi.big.webgraph.algo.StandardBreadthFirst;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.BitSet;
import java.util.stream.LongStream;

import static it.unimi.dsi.big.webgraph.benchmarks.BenchmarkUtils.*;

/**
 *
 * Class for comparing a Parallel standard implementation of a Breadth-first-search
 * and the Multi-source breadth-first-search.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class StandardBFSVsMSBFS {

    private final int maxNumberOfSources = 3000;
    private final int sourceBulkSize = 1000;
    private final int startNode = 0;
    private final int maxSteps = 3;
    private final String dateString = getDateString();

    private final String graphName = "in-2004";
    private final String graphFile = graphFolder + graphName;
    private final String dataFile  = dataFolder + "benchmarkBfs" + dateString + ".data";

    private final int nrSamples = 3;

    /**
     * Loads a physical graph and performs both a standard bfs and a msbfs on it.
     * Their completion time is measured and saved into a data file.
     * The BFS's will originate from the first nodes in the graph.
     * @throws IOException
     * @throws InterruptedException
     */
    public void benchmark() throws IOException, InterruptedException {

        ImmutableGraphWrapper graph = new ImmutableGraphWrapper(ImmutableGraph.loadMapped(graphFile));

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("%" + getDateString() + " " + graphName + "; Comparison between a standard implementation of BFS and the MS-BFS algorithm; "
                + " 0 to " + maxNumberOfSources + " sources are used for each bfs with " + sourceBulkSize + " bulk increase;" +
                " The time measured is the millis to perform a bfs that stops after h steps;");
        writer.println("%h nrSources stdbfsMillis msbfsMillis");

        int nrSources = sourceBulkSize;
        long[] sources = LongStream.range(startNode, startNode + nrSources).toArray();

        /* WARMUP CPU */
        for (int i = 0; i < nrSamples; i++) {
            performMSBfsAndMeasureTime(sources, graph, 2);
            performStandardBfsAndMeasureTime(sources, graph, 1);
        }


        while(nrSources <= maxNumberOfSources) {
            sources = LongStream.range(startNode, startNode + nrSources).toArray();

            for (int h = 0; h <= maxSteps; h++) {

                long msbfsTotalTime = 0;
                long stdTotalTime = 0;

                for (int i = 0; i < nrSamples; i++) {
                    msbfsTotalTime += performMSBfsAndMeasureTime(sources, graph, h);
                    stdTotalTime += performStandardBfsAndMeasureTime(sources, graph, h);
                }

                msbfsTotalTime = msbfsTotalTime / nrSamples;
                stdTotalTime = stdTotalTime / nrSamples;

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
    private static long performStandardBfsAndMeasureTime(long[] sources, MutableGraph graph, int h) {
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
    private static long performMSBfsAndMeasureTime(long[] sources, MutableGraph graph, final int h) throws InterruptedException {
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

    public static void main(String[] args) throws IOException, InterruptedException {
        new StandardBFSVsMSBFS().benchmark();
    }
}
