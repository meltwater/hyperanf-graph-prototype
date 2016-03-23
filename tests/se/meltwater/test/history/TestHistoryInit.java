package se.meltwater.test.history;

import org.junit.Test;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

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

    @Test
    /**
     * Tests that after creating a DANF and adding history to it
     * we can access all nodes history properly.
     */
    public void testAddHistory() throws IOException {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {

            setupRandomParameters();
            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);
            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            DANF danf = new DANF(dvc, h, graph);

            HyperLolLolCounterArray[] calculatedHistory = runHyperBallAndSaveEveryIteration(graph, danf, h, log2m);

            /* Check that the counters are correct for all nodes in all steps */
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
    }


    /**
     * Runs HyperBoll and for every iteration (which corresponds to a history level) we save the calculated counter.
     * The returned array can be used to compare against the histories added in {@code danf}
     * @param graph The graph to run HyperBoll on
     * @param danf The DANF to add history to
     * @param h The number of steps to run
     * @param log2m -
     * @return A h long array where each index i is the counter from the i'th step of HyperBoll.
     * @throws IOException
     */
    private HyperLolLolCounterArray[] runHyperBallAndSaveEveryIteration(IGraph graph, DANF danf, int h, int log2m) throws IOException {
        HyperBoll hyperBoll = new HyperBoll(graph, log2m);
        HyperLolLolCounterArray[] calculatedHistory = new HyperLolLolCounterArray[h];
        hyperBoll.init();
        for (int i = 1; i <= h; i++) {
            hyperBoll.iterate();
            HyperLolLolCounterArray currentCounter = hyperBoll.getCounter();

            calculatedHistory[i-1] = (HyperLolLolCounterArray) currentCounter.clone();
            danf.addHistory(currentCounter, i);
        }

        hyperBoll.close();

        return calculatedHistory;
    }

    private void setupRandomParameters() {
        Random rand = new Random();
        log2m = rand.nextInt(10) + minLog2m;
        h = rand.nextInt(5) + minH;
    }
}
