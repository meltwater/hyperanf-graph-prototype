package se.meltwater.test.history;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import javafx.util.Pair;
import org.junit.Test;
import static org.junit.Assert.*;

import se.meltwater.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.io.IOException;
import java.util.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class for testing various aspects of the DANF class
 * such as its connections with a Dynamic Vertex cover and
 * HyperBoll.
 */
public class TestDANF {

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

    @Test
    /**
     * N3 -> 0 -> 1
     * v --> 2
     */
    public void testNewNodeGetsCorrectRecalculation() throws IOException, InterruptedException {
        log2m = 10;
        h = 3;

        final int newNode = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);
        graph.addNode(1);
        graph.addNode(2);
        graph.addEdge(new Edge(0,1));

        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        Pair<DANF, HyperBoll> pair = TestUtils.runHyperBall(graph, dvc, h, log2m, fixedSeed);

        DANF danf = pair.getKey();

        danf.addEdges(new Edge(newNode, 0), new Edge(newNode, 2));

        final double expectedValue = 4.0;
        final double counterValue = danf.count(newNode, h);

        assertEquals(expectedValue, counterValue , epsilon);
    }

    @Test
    /**
     * N1 <-> N2
     */
    public void testTwoNewNodesCircleReference() throws IOException, InterruptedException {
        log2m = 10;
        h = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0); /* Must be a node in the graph for HBoll */

        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        Pair<DANF, HyperBoll> pair = TestUtils.runHyperBall(graph, dvc, h, log2m, fixedSeed);

        DANF danf = pair.getKey();

        danf.addEdges(new Edge(1, 2), new Edge(2, 1));

        assertEquals(2.0, danf.count(1, h), epsilon);
        assertEquals(2.0, danf.count(2, h), epsilon);
    }

    @Test
    /**
     * Tests that a complete recalculation of a node gives the same
     * result as HyperBoll would.
     */
    public void historyUnchangedOnRecalculation() throws IOException, InterruptedException {
        setupRandomParameters();

        BVGraph bvGraph = BVGraph.load("testGraphs/wordassociationNoBlocks");
        IGraph graph = new ImmutableGraphWrapper(bvGraph);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        DANF danf = TestUtils.runHyperBall(graph, dvc, h, log2m).getKey();
        assertCurrentCountIsSameAsRecalculatedCount(danf,dvc);

    }

    @Test
    /**
     * Tests that a node which previously didn't have any edges has some history after
     * adding edges
     */
    public void historyIncreaseOnAddedEdge() throws IOException, InterruptedException {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            setupRandomParameters();

            SimulatedGraph graph = TestUtils.genRandomGraph(100);
            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            Pair<DANF, HyperBoll> pair = TestUtils.runHyperBall(graph, dvc, h, log2m);

            addEdgeAndAssertIncreasedCount(pair.getKey(), pair.getValue(), dvc, graph);
        }
    }

    public void addEdgeAndAssertIncreasedCount(DANF nh, HyperBoll boll, IDynamicVertexCover vc, SimulatedGraph graph) throws InterruptedException {


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
                } while(boll.getCounter().hasSameRegisters(node, neighbor));

                nh.addEdge(new Edge(node, neighbor));
                nh.recalculateHistory(node);
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

    @Test
    public void historyUnchangedCircleReference() throws IOException, InterruptedException {

        setupRandomParameters();

        IGraph graph = createGraphWithCircles();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        DANF danf = TestUtils.runHyperBall(graph, dvc, h, log2m).getKey();
        assertCurrentCountIsSameAsRecalculatedCount(danf,dvc);

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
     * Tests that all recalculations give the same answer as the previous history
     * @throws InterruptedException
     */
    private void assertCurrentCountIsSameAsRecalculatedCount(DANF danf, DynamicVertexCover dvc) throws InterruptedException {
        double[] history;
        for(long node : dvc.getNodesInVertexCover()){
            if(node == 83)
                System.out.println("YOYO");
            history = danf.count(node);
            danf.recalculateHistory(node);

            assertArrayEquals("Failed for node " + node,history, danf.count(node), 0.01);
        }
    }

    @Test
    /**
     * Randomized test that adds edges with one new node in each edge.
     * Tests that each new node gets assigned a counter that can be accessed.
     */
    public void testAddedEdgesGetMemoryInHistory() throws Exception {
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            setupRandomParameters();

            IGraph graph = setupRandomGraph();
            DynamicVertexCover dvc = new DynamicVertexCover(graph);
            DANF danf = TestUtils.runHyperBall(graph, dvc, h, log2m).getKey();

            Set<Long> addedNodes = addRandomEdgesWithUniqueFromNodes(graph, danf);

            assertNodesCanBeAccessed(danf, dvc, addedNodes);
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


            danf.addEdge(generatedEdge);
            addedNodes.add(generatedEdge.from);
        }

        return addedNodes;
    }



    /**
     * Tests that all nodes in the list can be accessed.
     * @param nodes
     */
    private void assertNodesCanBeAccessed(DANF danf, DynamicVertexCover dvc, Set<Long> nodes) {
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
        log2m = 7;//rand.nextInt(10) + minLog2m;
        h = 5;//rand.nextInt(5) + minH;
    }


}


