package se.meltwater.test;

import it.unimi.dsi.big.webgraph.BVGraph;
import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.NodeHistory;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class for testing various aspects of the NodeHistory class
 * such as its connections with a Dynamic Vertex cover and
 * HyperBoll.
 */
public class TestNodeHistory {

    final int nrTestIterations = 100;
    final int maxStartNodes = 100;
    final int minLog2m = 4;
    final int minH = 2;
    final int edgesToAdd = 100;

    int log2m;
    int h;

    @Test
    /**
     * Tests that a complete recalculation of a node gives the same
     * result as HyperBoll would.
     */
    public void historyUnchangedOnRecalculation() throws IOException, InterruptedException {
        setupRandomParameters();

        BVGraph bvGraph = BVGraph.load("testGraphs/wordassociationNoBlocks");
        IGraph graph = new ImmutableGraphWrapper(bvGraph);
        DynamicVertexCover dvc = TestUtils.setupDVC(graph);

        NodeHistory nodeHistory = runHyperBall(graph, dvc, h);
        assertCurrentCountIsSameAsRecalculatedCount(nodeHistory, dvc);
    }

    /**
     * Tests that all recalculations give the same answer as the previous history
     * @throws InterruptedException
     */
    private void assertCurrentCountIsSameAsRecalculatedCount(NodeHistory nodeHistory, DynamicVertexCover dvc) throws InterruptedException {
        double[] history;
        for(long node : dvc.getNodesInVertexCover()){
            history = nodeHistory.count(node);
            nodeHistory.recalculateHistory(node);
            assertArrayEquals("Failed for node " + node,history,nodeHistory.count(node), 0.01);
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
            DynamicVertexCover dvc = TestUtils.setupDVC(graph);
            NodeHistory nodeHistory = runHyperBall(graph, dvc, h);

            Set<Long> addedNodes = addRandomEdgesWithUniqueFromNodes(graph, nodeHistory);

            assertNodesCanBeAccessed(nodeHistory, dvc, addedNodes);
        }
    }

    /**
     * Randomly generates a set of edges from new unique indexes to existing nodes.
     * Adds every generated node to {@code nodeHistory}.
     * @throws InterruptedException
     * @return A list of all new nodes created
     */
    private Set<Long> addRandomEdgesWithUniqueFromNodes(IGraph graph, NodeHistory nodeHistory) throws InterruptedException {
        Set<Long> addedNodes = new HashSet<>();
        int edgesAdded = 0;
        long n = graph.getNumberOfNodes();
        while (edgesAdded++ < edgesToAdd) {
            Random rand = new Random();

            long from = n;
            long to = (long) rand.nextInt((int) n);
            n++;
            Edge generatedEdge = new Edge(from, to);

            nodeHistory.addEdge(generatedEdge);
            addedNodes.add(generatedEdge.from);
        }

        return addedNodes;
    }

    /**
     * Tests that all nodes in the list can be accessed.
     * @param nodes
     */
    private void assertNodesCanBeAccessed(NodeHistory nodeHistory, DynamicVertexCover dvc, Set<Long> nodes) {
        for (Long addedNode : nodes) {
            if (dvc.isInVertexCover(addedNode)) {
                assertTrue(nodeHistory.count(addedNode) != null);
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

    /**
     * Runs hyperBoll on the graph and returns the calculated NodeHistory
     * @param graph
     * @param dvc
     * @param h
     * @return
     * @throws IOException
     */
    private NodeHistory runHyperBall(IGraph graph, DynamicVertexCover dvc, int h) throws IOException {
        NodeHistory nodeHistory = new NodeHistory(dvc, h, graph);
        HyperBoll hyperBoll = new HyperBoll(graph, log2m);
        hyperBoll.init();
        for (int i = 1; i < h; i++) {
            hyperBoll.iterate();
            nodeHistory.addHistory(hyperBoll.getCounter(), i);
        }

        hyperBoll.close();
        return nodeHistory;
    }
}


