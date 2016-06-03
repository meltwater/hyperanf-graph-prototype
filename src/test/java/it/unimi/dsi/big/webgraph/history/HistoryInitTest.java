package it.unimi.dsi.big.webgraph.history;

import it.unimi.dsi.big.webgraph.SimulatedGraph;
import it.unimi.dsi.big.webgraph.TestUtils;
import it.unimi.dsi.big.webgraph.algo.DANF;
import it.unimi.dsi.big.webgraph.algo.HyperBall;
import it.unimi.dsi.big.webgraph.algo.IDynamicVertexCover;
import it.unimi.dsi.util.HyperLogLogCounterArray;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class HistoryInitTest {

    final int nrTestIterations = 100;

    final int maxNodes = 100;
    final float epsilon = 0.05f;

    final int minLog2m = 4;
    int log2m;

    final int minH = 4;
    int h;

    /**
     * Tests that after creating a DANF and adding history to it
     * we can access all nodes history properly.
     */
    @Test
    public void testAddHistory() throws IOException {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {

            setupRandomParameters();
            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);

            DANF danf = new DANF(h, log2m, graph);

            HyperLogLogCounterArray[] calculatedHistory = getCorrectCounters(graph, danf);
            checkNodeCountersCorrect(graph, danf, calculatedHistory);

            danf.close();

        }
    }

    /**
     * Uses HyperBall to calculate the correct counters for all nodes in all steps.
     * @param graph
     * @param danf
     * @return
     * @throws IOException
     */
    private HyperLogLogCounterArray[] getCorrectCounters(SimulatedGraph graph, DANF danf) throws IOException {
        HyperLogLogCounterArray[] calculatedHistory = new HyperLogLogCounterArray[h];
        HyperBall hyperBall = new HyperBall(graph,log2m,danf.getCounter(h).getJenkinsSeed());
        hyperBall.init();
        for (int i = 1; i <= h ; i++) {
            hyperBall.iterate();
            calculatedHistory[i-1] = (HyperLogLogCounterArray) hyperBall.getCounter().clone();
        }
        hyperBall.close();
        return calculatedHistory;
    }

    /**
     * Check that the counters are correct for all nodes in all steps
     *
     * @param graph
     * @param danf
     * @param calculatedHistory
     */
    private void checkNodeCountersCorrect(SimulatedGraph graph, DANF danf, HyperLogLogCounterArray[] calculatedHistory) {

        IDynamicVertexCover dvc = danf.getDynamicVertexCover();
        for (int i = 1; i <= h; i++) {
            for (int node = 0; node < graph.numNodes(); node++) {
                if (dvc.isInVertexCover(node) || i == h) {
                    /* For all i != h danf will have the node mapped to another index, so we make sure
                     * we get the same value from that mapped index. For i == h all nodes should be
                      * in danf and have a value*/
                    assertEquals(calculatedHistory[i - 1].count(node), danf.count(node, i), epsilon);
                } else {
                    final int constNode = node; /* Final required for lambda function, uhh */
                    final int constI = i;
                    /* Nodes outside VC shouldnt have any values for i != h and should give exception */
                    TestUtils.assertGivesException(() -> danf.count(constNode, constI));
                }
            }
        }
    }

    private void setupRandomParameters() {
        Random rand = new Random();
        log2m = rand.nextInt(10) + minLog2m;
        h = rand.nextInt(5) + minH;
    }
}
