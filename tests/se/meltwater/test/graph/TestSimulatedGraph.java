package se.meltwater.test.graph;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.junit.Test;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;

import java.util.*;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class for testing that {@link SimulatedGraph} works
 * gives the same results as {@link ImmutableGraphWrapper}
 */
public class TestSimulatedGraph {

    final int nrTestIterations = 100;
    final int maxNodes = 100;

    /**
     * Generates a random graph and tests that Simulated Graph
     * returns the correct number of arcs.
     */
    @Test
    public void testSimulatedGraphCorrectNumArcs() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            final int maxNumNodes = 100;
            Set<Long> nodesSet = new HashSet<>();
            Map<Long, Set<Long>> edges = new HashMap<>();


            IGraph graph = TestUtils.genRandomGraph(maxNumNodes);
            buildNodeAndEdgeSetFromGraph(nodesSet, edges, graph);

            long edgeCount = 0;
            for (Map.Entry<Long, Set<Long>> entry : edges.entrySet()) {
                edgeCount += entry.getValue().size();
            }

            assertEquals(graph.getNumberOfArcs(), edgeCount);
        }
    }

    /**
     * Iterates the graph and calculates the unique set of nodes and the unique
     * edges present in the graph.
     * @param nodesSet An empty set that later will include all unique nodes
     * @param edges An emtpy map that later will include a mapping between nodes and their neighbors (calculated from outgoing edges)
     * @param graph The graph to build from
     */
    private void buildNodeAndEdgeSetFromGraph(Set<Long> nodesSet, Map<Long, Set<Long>> edges, IGraph graph) {
        graph.iterateAllEdges(edge -> {
            nodesSet.add(edge.from);
            nodesSet.add(edge.to);

            Set<Long> outEdges = edges.get(edge.from);
            if(outEdges == null) {
                outEdges = new HashSet<>();
                edges.put(edge.from, outEdges);
            }

            outEdges.add(edge.to);

            return null;
        });
    }

    /**
     * Tests that deleting non-existing edges dont affect the graph
     */
    @Test
    public void testDeleteNonExistingEdge() {
        final int maxNodes = 100;
        final int maxNewEdges = 100;

        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            Random rand = new Random();
            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);
            long arcCountBefore = graph.getNumberOfArcs();

            for (int i = 0; i < maxNewEdges; i++) {
                long from = rand.nextInt() + graph.getNumberOfNodes() + 1;
                long to = rand.nextInt() + graph.getNumberOfNodes() + 1;
                Edge newEdge = new Edge(from, to);

                boolean wasDeleted = graph.deleteEdge(newEdge);
                assertFalse(wasDeleted);
            }

            assertEquals(arcCountBefore, graph.getNumberOfArcs());
        }
    }

    /**
     * Tests that an empty graph returns the correct numNodes
     * and numArcs
     */
    @Test
    public void testSimulatedGraphCorrectNumNodesZero() {
        IGraph graph = new SimulatedGraph();
        assertEquals(0, graph.getNumberOfNodes());
        assertEquals(0, graph.getNumberOfArcs());
    }

    /**
     * Tests that after inserting a node larger than MAX_INT
     * simulated graph returns the correct value of nodes.
     */
    @Test
    public void testSimulatedGraphCorrectNumNodesLarge() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = new SimulatedGraph();

            Random rand = new Random();
            int exponent = rand.nextInt(32) ;
            long longLargerThanInt = Integer.MAX_VALUE + (long) Math.pow(2, exponent);
            graph.addNode(longLargerThanInt);

            assertEquals(longLargerThanInt + 1, graph.getNumberOfNodes());
            assertEquals(0, graph.getNumberOfArcs());
        }
    }

    /**
     * Test that after some node insertions the graph gives the correct number
     * of nodes and arcs.
     */
    @Test
    public void testSimulatedGraphCorrectNumNodesSmall() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            final int maxNumNodes = 100;
            Random rand = new Random();
            int n = rand.nextInt(maxNumNodes);
            long[] nodes = LongStream.rangeClosed(0, n).toArray();
            Edge[] edges = new Edge[0];

            IGraph graph = TestUtils.setupSGraph(nodes, edges);

            assertEquals(nodes.length, graph.getNumberOfNodes());
            assertEquals(0, graph.getNumberOfArcs());
        }
    }



    /**
     * Tests that the SimulatedGraphIterator crashes when the graph has
     * no more nodes to iterate.
     */
    @Test
    public void testSimulatedGraphIteratorCrashWhenNoMoreNodes() {
        final int maxNodes = 100;
        SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);
        NodeIterator iterator = graph.getNodeIterator();

        int i = 0;
        while(i++ < graph.getNumberOfNodes()) {
            iterator.nextLong();
        }

        assertEquals(iterator.nextLong(),-1);
    }

    /**
     * Tests that the SimulatedGraphNodeIterator skips to the correct node
     */
    @Test
    public void testSimulatedGraphIteratorSkipNodes() {
        final int maxNodes = 100;

        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);
            NodeIterator iterator = graph.getNodeIterator();

            Random rand = new Random();
            long skipTo = rand.nextInt((int) graph.getNumberOfNodes());

            long node = iterator.skip(skipTo);

            assertEquals(node, skipTo);
        }
    }

    /**
     *
     */
    @Test
    public void testSuccessorIterator() {
        final int maxNodes = 100;

        int iteration = 0;
        while(iteration++ < nrTestIterations) {

            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);

            for (int i = 0; i < graph.getNumberOfNodes(); i++) {
                long nodeDegree = graph.getOutdegree(i);
                if (nodeDegree > 0) {
                    LazyLongIterator successors = graph.getSuccessors(i);
                    successors.skip(nodeDegree);

                    assertEquals(successors.nextLong(),-1);
                } else {
                    LazyLongIterator successors = graph.getSuccessors(i);

                    assertEquals(successors.nextLong(),-1);
                }
            }
        }
    }


    @Test
    public void testEquals() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);
            assertTrue(graph.equals(graph));
        }
    }


    @Test
    public void testEqualsClone() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);
            assertEquals(graph, graph.clone());
        }
    }

    /**
     * Tests that all edges are present and flipped in the transpose
     */
    @Test
    public void testTranspose() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {

            SimulatedGraph graph = TestUtils.genRandomGraph(maxNodes);

            IGraph transpose = graph.transpose();

            assertEquals(graph.getNumberOfArcs(), transpose.getNumberOfArcs());
            assertEquals(graph.getNumberOfNodes(), transpose.getNumberOfNodes());

            ArrayList<Edge> edges = new ArrayList<>(Arrays.asList(graph.getAllEdges()));

            transpose.iterateAllEdges(edge -> {
                Edge flippedEdge = new Edge(edge.to, edge.from);
                boolean wasRemoved = edges.remove(flippedEdge);
                assertTrue(wasRemoved);
                return null;
            });

            assertTrue(edges.size() == 0);
        }
    }

    /**
     * Tests that we dont crash when transposing an empty graph
     */
    @Test
    public void testTransposeEmptyGraph() {
        SimulatedGraph graph = new SimulatedGraph();
        IGraph tranpose = graph.transpose();
        assertTrue(tranpose != null);
    }

    /**
     * Transposing twice should give back the same graph
     */
    @Test
    public void testTransposeTwice() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            IGraph graph = TestUtils.genRandomGraph(maxNodes);
            IGraph twiceTransposedGraph = graph.transpose().transpose();

            assertEquals(graph.getNumberOfArcs(), twiceTransposedGraph.getNumberOfArcs());
            assertEquals(graph.getNumberOfNodes(), twiceTransposedGraph.getNumberOfNodes());
            assertEquals(graph, twiceTransposedGraph);
        }
    }




}
