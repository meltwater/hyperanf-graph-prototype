package it.unimi.dsi.big.webgraph.benchmarks;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.ImmutableGraphWrapper;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.big.webgraph.algo.DANF;
import it.unimi.dsi.big.webgraph.algo.TrivialDynamicANF;

import java.io.IOException;
import java.io.PrintWriter;

import static it.unimi.dsi.big.webgraph.benchmarks.BenchmarkUtils.*;

/**
 *
 * Benchmarks the difference between the DANF implementation
 * and the Trivial 2-bfs algorithm described in the publication
 * DANF: Approximate Neighborhood Function on Large Dynamic Graphs.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class DanfVsTrivial {

    private final String dateString = getDateString();
    private final String dataFile = dataFolder + "DANFComparedToTrivial" + dateString + ".data";
    private final String graphName = "in-2004";
    private final String graphFile = graphFolder + graphName;

    private final int log2m = 7;
    private final int h = 3;
    private final int maxBulkSize = 6400;
    private int bulkSize =      640;
    private final int bulkSizeIncrease = 640;

    private int  added = 0;
    private long totalDanfTime = 0;
    private long totalTrivialTime = 0;
    private long start = System.currentTimeMillis();

    private float danfGraphGB;
    private float danfCounterGB;
    private float danfVCGB;
    private float danfMSBFSGB;
    private float danfTotalMemory;
    private float danfEps;

    private float trivialEps;
    private float trivialTotalMemory;


    public void benchmark() throws IOException, InterruptedException {

        MutableGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));
        MutableGraph graph2 = new ImmutableGraphWrapper(BVGraph.loadMapped(graphFile));

        DANF danf = new DANF(h,log2m,graph);
        TrivialDynamicANF tanf = new TrivialDynamicANF(h,log2m,graph2);

        PrintWriter writer = new PrintWriter(dataFile);
        writer.println("%" + dateString + " " + graphName + " " + maxBulkSize + " edges will be inserted into DANF in bulks of " +
                bulkSize + ". The time measured is the time to insert " + bulkSize + " edges.; h is set to " + h + " and log2m is " + log2m + ";");
        writer.println("%BulkSize DanfEPS DanfGraphMemory DanfCounterMemory DanfVcMemory DanfMsbfsMemory TrivialEPS TrivialMemory nrArcs nrNodes");

        while(bulkSize <= maxBulkSize) {
            Edge[] edges = generateEdges(graph.numNodes(), bulkSize);

            AddEdgesIntoDanfAndUpdateMeasurements(danf, edges);
            addEdgesIntoTanfAndUpdateMeasurements(tanf, edges);

            writer.println(bulkSize + " " + danfEps + " " + danfGraphGB+ " " + danfCounterGB + " " + danfVCGB + " " + danfMSBFSGB
                    + " " + trivialEps + " " + trivialTotalMemory  + " " + added + " " + graph.numNodes());
            writer.flush();

            System.out.println(bulkSize + " edges, " + danfEps + " Danf DPS, " + danfTotalMemory + "GB DANF memory, " + trivialEps + " Trivial DPS, " +
                    trivialTotalMemory + "GB trivial memory, " + added + " " + graph.numNodes());

            added += bulkSize;
            bulkSize += bulkSizeIncrease;
        }

        danf.close();
        writer.close();
    }

    private void addEdgesIntoTanfAndUpdateMeasurements(TrivialDynamicANF tanf, Edge[] edges) {
        long beforeTrivial = System.currentTimeMillis();
        tanf.addEdges(edges);
        long afterTrivial = System.currentTimeMillis();

        totalTrivialTime += afterTrivial-beforeTrivial;
        trivialEps = (float) added / totalTrivialTime * 1000;

        trivialTotalMemory = tanf.getMemoryUsageBytes()/(float)bytesPerGigaByte;
    }

    private void AddEdgesIntoDanfAndUpdateMeasurements(DANF danf, Edge[] edges) {
        long beforeDanf = System.currentTimeMillis();
        danf.addEdges(edges);
        long afterDanf = System.currentTimeMillis();

        totalDanfTime += afterDanf-beforeDanf;
        danfEps = (float) added / totalDanfTime * 1000;

        danfGraphGB = danf.getMemoryUsageGraphBytes()/(float)bytesPerGigaByte ;
        danfCounterGB = danf.getMemoryUsageCounterBytes()/(float)bytesPerGigaByte;
        danfVCGB = danf.getMemoryUsageVCBytes() / (float) bytesPerGigaByte;
        danfMSBFSGB = danf.getMemoryUsageMsBfsBytes() / (float) bytesPerGigaByte;
        danfTotalMemory = danfGraphGB + danfCounterGB + danfVCGB + danfMSBFSGB;
    }
}
