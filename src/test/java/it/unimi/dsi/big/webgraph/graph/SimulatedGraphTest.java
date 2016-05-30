package it.unimi.dsi.big.webgraph.graph;

import com.google.common.collect.Lists;
import it.unimi.dsi.big.webgraph.*;
import org.junit.Test;

import java.io.IOException;
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
public class SimulatedGraphTest {

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


            MutableGraph graph = TestUtils.genRandomGraph(maxNumNodes);
            buildNodeAndEdgeSetFromGraph(nodesSet, edges, graph);

            long edgeCount = 0;
            for (Map.Entry<Long, Set<Long>> entry : edges.entrySet()) {
                edgeCount += entry.getValue().size();
            }

            assertEquals(graph.numArcs(), edgeCount);
        }
    }

    /**
     * Iterates the graph and calculates the unique set of nodes and the unique
     * edges present in the graph.
     * @param nodesSet An empty set that later will include all unique nodes
     * @param edges An emtpy map that later will include a mapping between nodes and their neighbors (calculated from outgoing edges)
     * @param graph The graph to build from
     */
    private void buildNodeAndEdgeSetFromGraph(Set<Long> nodesSet, Map<Long, Set<Long>> edges, MutableGraph graph) {
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
            long arcCountBefore = graph.numArcs();

            for (int i = 0; i < maxNewEdges; i++) {
                long from = rand.nextInt() + graph.numNodes() + 1;
                long to = rand.nextInt() + graph.numNodes() + 1;
                Edge newEdge = new Edge(from, to);

                boolean wasDeleted = graph.deleteEdge(newEdge);
                assertFalse(wasDeleted);
            }

            assertEquals(arcCountBefore, graph.numArcs());
        }
    }

    /**
     * Tests that an empty graph returns the correct numNodes
     * and numArcs
     */
    @Test
    public void testSimulatedGraphCorrectNumNodesZero() {
        MutableGraph graph = new SimulatedGraph();
        assertEquals(0, graph.numNodes());
        assertEquals(0, graph.numArcs());
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

            assertEquals(longLargerThanInt + 1, graph.numNodes());
            assertEquals(0, graph.numArcs());
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

            MutableGraph graph = TestUtils.setupSGraph(nodes, edges);

            assertEquals(nodes.length, graph.numNodes());
            assertEquals(0, graph.numArcs());
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
        NodeIterator iterator = graph.nodeIterator();

        int i = 0;
        while(i++ < graph.numNodes()) {
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
            NodeIterator iterator = graph.nodeIterator();

            Random rand = new Random();
            long skipTo = rand.nextInt((int) graph.numNodes());

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

            for (int i = 0; i < graph.numNodes(); i++) {
                long nodeDegree = graph.outdegree(i);
                if (nodeDegree > 0) {
                    LazyLongIterator successors = graph.successors(i);
                    successors.skip(nodeDegree);

                    assertEquals(successors.nextLong(),-1);
                } else {
                    LazyLongIterator successors = graph.successors(i);

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

            MutableGraph transpose = graph.transpose();

            assertEquals(graph.numArcs(), transpose.numArcs());
            assertEquals(graph.numNodes(), transpose.numNodes());

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
        MutableGraph tranpose = graph.transpose();
        assertTrue(tranpose != null);
    }

    /**
     * Transposing twice should give back the same graph
     */
    @Test
    public void testTransposeTwice() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            MutableGraph graph = TestUtils.genRandomGraph(maxNodes);
            MutableGraph twiceTransposedGraph = graph.transpose().transpose();

            assertEquals(graph.numArcs(), twiceTransposedGraph.numArcs());
            assertEquals(graph.numNodes(), twiceTransposedGraph.numNodes());
            assertEquals(graph, twiceTransposedGraph);
        }
    }

    /**
     * Tests that the added edges to the graph exists, the old ones remain, and no
     * edges are created from nothing.
     * @throws IOException
     */
    @Test
    public void testAddedEdgesCorrect() throws IOException {
        SimulatedGraph graph = new SimulatedGraph();
        int numNodes;
        Random rand = new Random();

        for(int i = 0; i< 100; i++) {

            numNodes = rand.nextInt(maxNodes)+1;
            ArrayList<Edge> edgesBefore = getEdgesBefore(graph);

            ArrayList<Edge> newEdges = generateNewEdges(numNodes,graph);
            removeEdgesAppearingInGraph(edgesBefore, newEdges,graph);
            assertEquals(0,newEdges.size());
            assertEquals(0,edgesBefore.size());

        }

    }

    private void removeEdgesAppearingInGraph(ArrayList<Edge> edgesBefore, ArrayList<Edge> newEdges, SimulatedGraph graph) {
        graph.iterateAllEdges(e ->{
            assertTrue(newEdges.contains(e) || edgesBefore.contains(e));
            while (newEdges.remove(e)) ;
            while (edgesBefore.remove(e));
            return null;
        });

    }

    private ArrayList<Edge> generateNewEdges(int numNodes, SimulatedGraph graph) {
        int numEdges = maxNodes;
        ArrayList<Edge> edges = Lists.newArrayList(TestUtils.generateEdges(numNodes, numEdges));

        Edge[] edgeArr = edges.toArray(new Edge[edges.size()]);
        graph.addEdges(edgeArr);
        return edges;
    }

    private ArrayList<Edge> getEdgesBefore(SimulatedGraph graph) {
        ArrayList<Edge> edgesBefore = new ArrayList<>();
        graph.iterateAllEdges(e -> {
            edgesBefore.add(e);
            return null;
        });
        return edgesBefore;
    }


}
