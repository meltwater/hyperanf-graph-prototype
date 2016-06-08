package it.unimi.dsi.big.webgraph.benchmarks;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.ImmutableGraphWrapper;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.big.webgraph.algo.DANF;
import it.unimi.dsi.big.webgraph.algo.DynamicNeighborhoodFunction;

import java.io.IOException;
import java.io.PrintWriter;

import static it.unimi.dsi.big.webgraph.benchmarks.BenchmarkUtils.*;


/**
 *
 * Class for benchmarking different aspects of DANF
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class DanfBenchmark {

    private final String dateString = getDateString();
    private final String dataFile = dataFolder + "EdgeInsertionsDanfReal" + dateString + ".data";
    private final String graphName = "noBlocksUk";
    private final String graphFile = graphFolder + graphName;

    private final int log2m = 7;
    private final int h = 3;
    private final int edgesToAdd =   1000000;
    private final int bulkSize =       10000;

    /**
     * Benchmarks the time to insert edges into Dvanf using a real graph.
     * @throws IOException
     * @throws InterruptedException
     */
    public void benchmark() throws IOException, InterruptedException {


        System.out.println("Loading graph");
        MutableGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));

        DANF danf = new DANF(h,log2m,graph);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("#" + dateString + " " + graphName + " " + edgesToAdd + " edges will be inserted into DANF in bulks of " +
                bulkSize + ". The time measured is the time to insert " + bulkSize + " edges.; h is set to " + h + " and log2m is " + log2m + ";");
        writer.println("#Modifications DPS HeapSize ElapsedTime nrArcs nrNodes");

        insertEdgesIntoDanf(graph, danf, writer);

        writer.close();
    }

    /**
     * Inserts {@code edgesToAdd} random edges into {@code danf}
     * @param graph The graph in danf
     * @param danf
     * @param writer
     * @throws IOException
     * @throws InterruptedException
     */
    private void insertEdgesIntoDanf(MutableGraph graph, DynamicNeighborhoodFunction danf, PrintWriter writer) throws IOException, InterruptedException {
        int  added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;

        System.out.println("Starting edge insertions");
        while(System.in.available() == 0 && added < edgesToAdd) {
            Edge[] edges = generateEdges(graph.numNodes(), bulkSize);

            danf.addEdges(edges);
            added += bulkSize;

            long currentTime = System.currentTimeMillis();

            printAndLogStatistics(writer, added, lastTime, startTime, bulkSize, currentTime, graph, danf);
            lastTime = System.currentTimeMillis();
        }

        danf.close();
    }
}
