package se.meltwater.test.history;

import org.junit.Test;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class TestHistoryInit {

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

            HyperLolLolCounterArray[] calculatedHistory = getCorrectCounters(graph, danf);
            checkNodeCountersCorrect(graph, danf, calculatedHistory);


        }
    }

    /**
     * Uses HyperBoll to calculate the correct counters for all nodes in all steps.
     * @param graph
     * @param danf
     * @return
     * @throws IOException
     */
    private HyperLolLolCounterArray[] getCorrectCounters(SimulatedGraph graph, DANF danf) throws IOException {
        HyperLolLolCounterArray[] calculatedHistory = new HyperLolLolCounterArray[h];
        HyperBoll hyperBoll = new HyperBoll(graph,log2m,danf.getCounter(h).getJenkinsSeed());
        hyperBoll.init();
        for (int i = 1; i <= h ; i++) {
            hyperBoll.iterate();
            calculatedHistory[i-1] = (HyperLolLolCounterArray) hyperBoll.getCounter().clone();
        }
        return calculatedHistory;
    }

    /**
     * Check that the counters are correct for all nodes in all steps
     *
     * @param graph
     * @param danf
     * @param calculatedHistory
     */
    private void checkNodeCountersCorrect(SimulatedGraph graph, DANF danf, HyperLolLolCounterArray[] calculatedHistory) {

        IDynamicVertexCover dvc = danf.getDynamicVertexCover();
        for (int i = 1; i <= h; i++) {
            for (int node = 0; node < graph.getNumberOfNodes(); node++) {
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
