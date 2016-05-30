package se.meltwater.test.history;

import org.junit.Test;
import it.unimi.dsi.big.webgraph.algo.DANF;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.SimulatedGraph;
import se.meltwater.test.TestUtils;

import static org.junit.Assert.*;

/**
 * // TODO Class description
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class TestDanf {

    private final double epsilon = 0.05;
    private final int maxIterations = 100;

    private final long seed = -704687741363554677L;

    @Test
    public void testDanfMAddSameEdgesManyTimes() throws InterruptedException {
        final int log2m = 4;
        final int h = 5;
        final int nMax = 10;

        int i = 0;
        while(i++ < maxIterations) {

            SimulatedGraph graph = new SimulatedGraph();
            graph.addNode(0);
            DANF danf = new DANF(h, log2m, graph);

            SimulatedGraph graphToAdd = TestUtils.genRandomGraph(nMax);
            Edge[] edgesToAdd = graphToAdd.getAllEdges();

            danf.addEdges(edgesToAdd);
            double[] countersAfterFirstInsertion = new double[(int) graph.numNodes()];
            for (int node = 0; node < graph.numNodes(); node++) {
                countersAfterFirstInsertion[node] = danf.count(node, h);
            }

            danf.addEdges(edgesToAdd);
            double[] countersAfterSecondInsertion = new double[(int) graph.numNodes()];
            for (int node = 0; node < graph.numNodes(); node++) {
                countersAfterSecondInsertion[node] = danf.count(node, h);
            }

            for (int node = 0; node < graph.numNodes(); node++) {
                assertEquals(countersAfterFirstInsertion[node], countersAfterSecondInsertion[node], epsilon);
            }
            danf.close();
        }
    }

    @Test
    public void testStaticInsertSameEdgesManyTimes() throws InterruptedException {
        final int log2m = 4;
        final int h = 5;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);

        DANF danf = new DANF(h, log2m, graph, seed);
        danf.addEdges(new Edge(0, 2), new Edge(0, 6), new Edge(1, 3), new Edge(2, 8), new Edge(3, 2), new Edge(3, 3), new Edge(5, 4), new Edge(7, 2), new Edge(8, 5));

        double[] countersAfterFirstInsertion = new double[(int) graph.numNodes()];
        for (int node = 0; node < graph.numNodes(); node++) {
            countersAfterFirstInsertion[node] = danf.count(node, h);
        }

        danf.addEdges(new Edge(0, 2), new Edge(0, 6), new Edge(1, 3), new Edge(2, 8), new Edge(3, 2), new Edge(3, 3), new Edge(5, 4), new Edge(7, 2), new Edge(8, 5));
        double[] countersAfterSecondInsertion = new double[(int) graph.numNodes()];
        for (int node = 0; node < graph.numNodes(); node++) {
            countersAfterSecondInsertion[node] = danf.count(node, h);
        }

        for (int node = 0; node < graph.numNodes(); node++) {
            assertEquals(countersAfterFirstInsertion[node], countersAfterSecondInsertion[node], epsilon);
        }

        danf.close();
    }

}
