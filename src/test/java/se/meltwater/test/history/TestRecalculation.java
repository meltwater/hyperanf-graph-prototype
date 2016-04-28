package se.meltwater.test.history;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import javafx.util.Pair;
import org.junit.Test;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class for testing various aspects of the DANF class
 * such as its connections with a Dynamic Vertex cover and
 * HyperBoll.
 */
public class TestRecalculation {

    final float epsilon = 0.05f;

    final int nrTestIterations = 100;
    final int maxStartNodes = 100;
    final int minLog2m = 4;
    final int minH = 4;
    final int edgesToAdd = 100;

    /* For some tests its necessary to have a static seed to
     * make sure we always get the same result from HyperBoll */
    final long fixedSeed = 8516942932596937874L;

    int log2m;
    int h;

    /**
     * <pre>{@code
     * N3 -> 0 -> 1
     *  |
     *  v
     *  2
     *  }</pre>
     */
    @Test
    public void testNewNodeGetsCorrectRecalculation() throws IOException, InterruptedException {
        log2m = 10;
        h = 3;

        final int newNode = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);
        graph.addNode(1);
        graph.addNode(2);
        graph.addEdge(new Edge(0,1));

        DANF danf = new DANF(h,log2m,graph,fixedSeed);

        danf.addEdges(new Edge(newNode, 0), new Edge(newNode, 2));

        final double expectedValue = 4.0;
        final double counterValue = danf.count(newNode, h);

        assertEquals(expectedValue, counterValue , epsilon);

        danf.close();
    }

    /**
     * <pre>{@code
     * N1 <-> N2
     * }</pre>
     */
    @Test
    public void testTwoNewNodesCircleReference() throws IOException, InterruptedException {
        log2m = 10;
        h = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0); /* Must be a node in the graph for HBoll */

        DANF danf = new DANF(h,log2m,graph,fixedSeed);

        danf.addEdges(new Edge(2, 1), new Edge(1, 2));

        assertEquals(2.0, danf.count(1, h), epsilon);
        assertEquals(2.0, danf.count(2, h), epsilon);

        danf.close();
    }

    /**
     *
     * The graph:
     * <pre>{@code
     *  4N
     *  |
     *  v
     *  2
     *  |
     *  v
     *  0 --> 1
     *  ^
     *  |
     *  3
     *  ^
     *  |
     *  5N
     *  }</pre>
     */
    @Test
    public void testTwoNewNodesCorrectRecalculation() throws InterruptedException, IOException {
        log2m = 10;
        h = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(5); /* Must be a node in the graph for HBoll */
        graph.addEdge(new Edge(0,1));
        graph.addEdge(new Edge(3,0));
        graph.addEdge(new Edge(2,0));

        DANF danf = new DANF(h,log2m,graph,fixedSeed);

        danf.addEdges(new Edge(5,3),new Edge(4,2));

        assertEquals(4.0, danf.count(4, h), epsilon);
        assertEquals(4.0, danf.count(5, h), epsilon);

        danf.close();
    }

    /**
     * Tests that a node which previously didn't have any edges has some history after
     * adding edges
     */
    @Test
    public void historyIncreaseOnAddedEdge() throws IOException, InterruptedException {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            setupRandomParameters();

            SimulatedGraph graph = TestUtils.genRandomGraph(100);

            DANF danf = new DANF(h,log2m,graph);

            addEdgeAndAssertIncreasedCount(danf, graph);

            danf.close();
        }
    }

    public void addEdgeAndAssertIncreasedCount(DANF nh, SimulatedGraph graph) throws InterruptedException {

        IDynamicVertexCover vc = nh.getDynamicVertexCover();
        double[] history, history2;
        Random rand = new Random();
        LazyLongIterator it = vc.getNodesInVertexCoverIterator();
        for (long i = 0; i < vc.getVertexCoverSize() ; i++) {
            long node = it.nextLong();
            if(graph.getOutdegree(node) == 0) {
                history = nh.count(node);
                assertArrayEquals(repeat(1.0,history.length),history,0.05);
                long neighbor;

                do {
                    neighbor = rand.nextInt((int)graph.getNumberOfNodes());
                } while(nh.getCounter(h).hasSameRegisters(node, neighbor));

                nh.addEdges(new Edge(node, neighbor));
                history2 = nh.count(node);
                for (int j = 0; j < history.length; j++) {
                    assertTrue(history[j]+0.05 < history2[j]);
                }
            }
        }
    }

    public double[] repeat(double number, int times){
        double[] ret = new double[times];
        for (int i = 0; i < times ; i++) {
            ret[i] = number;
        }
        return ret;
    }

    private IGraph createGraphWithCircles(){
        SimulatedGraph g = new SimulatedGraph();
        g.addNode(1);
        g.addNode(4);
        g.addNode(0);
        g.addNode(2);
        g.addNode(3);
        g.addEdge(new Edge(0,1));
        g.addEdge(new Edge(0,2));
        g.addEdge(new Edge(0,3));
        g.addEdge(new Edge(0,4));
        g.addEdge(new Edge(2,0));
        g.addEdge(new Edge(3,4));
        return g;
    }

    /**
     * Randomized test that adds edges with one new node in each edge.
     * Tests that each new node gets assigned a counter that can be accessed.
     */
    @Test
    public void testAddedEdgesGetMemoryInHistory() throws Exception {
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            setupRandomParameters();

            IGraph graph = setupRandomGraph();
            DANF danf = new DANF(h,log2m,graph);

            Set<Long> addedNodes = addRandomEdgesWithUniqueFromNodes(graph, danf);

            assertNodesCanBeAccessed(danf, addedNodes);

            danf.close();
        }
    }

    /**
     * Randomly generates a set of edges from new unique indexes to existing nodes.
     * Adds every generated node to {@code danf}.
     * @throws InterruptedException
     * @return A list of all new nodes created
     */
    private Set<Long> addRandomEdgesWithUniqueFromNodes(IGraph graph, DANF danf) throws InterruptedException{
        Set<Long> addedNodes = new HashSet<>();
        int edgesAdded = 0;
        long n = graph.getNumberOfNodes();
        while (edgesAdded++ < edgesToAdd) {
            Random rand = new Random();

            long from = n;
            long to = (long) rand.nextInt((int) n);
            n++;
            Edge generatedEdge = new Edge(from, to);


            danf.addEdges(generatedEdge);
            addedNodes.add(generatedEdge.from);
        }

        return addedNodes;
    }



    /**
     * Tests that all nodes in the list can be accessed.
     * @param nodes
     */
    private void assertNodesCanBeAccessed(DANF danf, Set<Long> nodes) {
        IDynamicVertexCover dvc = danf.getDynamicVertexCover();
        for (Long addedNode : nodes) {
            if (dvc.isInVertexCover(addedNode)) {
                assertTrue(danf.count(addedNode) != null);
            }
        }
    }

    private IGraph setupRandomGraph() throws IOException {
        Random rand = new Random();
        int n = rand.nextInt(maxStartNodes);
        return TestUtils.genRandomGraph(n);
    }

    private void setupRandomParameters() {
        Random rand = new Random();
        log2m = rand.nextInt(10) + minLog2m;
        h = rand.nextInt(5) + minH;
    }


}


