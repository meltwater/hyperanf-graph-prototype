package se.meltwater.benchmarks;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.ImmutableGraphWrapper;

import java.io.IOException;

import static se.meltwater.benchmarks.BenchmarkUtils.graphFolder;

/**
 *
 * Class for performing benchmark comparisons updating DANF values
 * and performing HyperBall recalculations.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class DanfVsHanf {

    private final int log2m = 4;
    private final int h = 3;
    private int bulkSize = 30000;
    private final long seed = 0L;

    private final String graphName = "ljournal-2008";
    private final String graphFile = graphFolder + graphName;

    private int nrSamples = 10;

    private long danfTotalTime = 0;
    private long hBallTotalTime = 0;

    private float memoryThreasholdThatNeverReaches = 999999;

    /**
     * Performs a benchmark comparing the time to perform a DANF update and a
     * HyperBall recalculation
     * @throws IOException
     */
    public void benchmark() throws IOException {
        for (int i = 0; i < nrSamples; i++) {
            ImmutableGraphWrapper graph  = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile), memoryThreasholdThatNeverReaches);

            Edge[] edges = BenchmarkUtils.generateEdges(graph.getNumberOfNodes(), bulkSize);

            danfTotalTime += addEdgesToDanf(graph, edges);
            hBallTotalTime += addEdgesToHyperBall(edges);

            System.out.println("Currently, Danf: " + danfTotalTime / (i+1) + " HBall: " + hBallTotalTime / (i+1));
        }

        System.out.println("DANF Total: " + danfTotalTime / nrSamples + " ms, HBALL Total: " + hBallTotalTime / nrSamples + " ms.");
    }

    /**
     * Inserts {@code edges} into {@code graph}
     * @param graph The graph to insert the edges into
     * @param edges The edges to insert
     * @return The elapsed time in ms to perform a danf update
     */
    private long addEdgesToDanf(ImmutableGraphWrapper graph,  Edge[] edges) {
        DANF danf = new DANF(h, log2m, graph , seed);
        System.out.println("Running DANF");
        long beforeDanf = System.currentTimeMillis();
        danf.addEdges(edges);
        long afterDanf = System.currentTimeMillis();
        long danfTotalTime = afterDanf - beforeDanf;

        /* Cleanup */
        danf.close();
        danf = null;
        graph.close();
        graph = null;

        return danfTotalTime;
    }

    /**
     * Inserts {@code edges} into {@code graphFile} graph.
     * @param edges The edges to insert
     * @return The elapsed time in ms to perform a HyperBall calculation
     * @throws IOException
     */
    private long addEdgesToHyperBall(Edge[] edges) throws IOException {
        ImmutableGraphWrapper graph2 = new ImmutableGraphWrapper(ImmutableGraph.load(graphFile), memoryThreasholdThatNeverReaches);
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

        /* Cleanup */
        graph2.close();
        graph2 = null;

        return afterHBALL - beforeHBALL;
    }
}
