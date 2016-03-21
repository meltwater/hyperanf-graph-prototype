package se.meltwater.test.graph;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.junit.Test;
import static org.junit.Assert.*;

import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.LongStream;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class for testing that {@link SimulatedGraph} works
 * gives the same results as {@link ImmutableGraphWrapper}
 */
public class TestSimulatedGraph {

    final int nrTestIterations = 100;

    @Test
    /**
     * Generates a random graph and tests that Simulated Graph
     * returns the correct number of arcs.
     */
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

    @Test
    /**
     * Tests that an emptry graph returns the correct numNodes
     * and numArcs
     */
    public void testSimulatedGraphCorrectNumNodesZero() {
        IGraph graph = new SimulatedGraph();
        assertEquals(0, graph.getNumberOfNodes());
        assertEquals(0, graph.getNumberOfArcs());
    }

    @Test
    /**
     * Tests that after inserting a node larger than MAX_INT
     * simulated graph returns the correct value of nodes.
     */
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

    @Test
    /**
     * Test that after some node insertions the graph gives the correct number
     * of nodes and arcs.
     */
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

    @Test
    /**
     * Tests that both a real loaded graph and a similated graph clone of it
     * have the same number of arcs and nodes. Also checks that ImmutableGraphWrapper
     * have the same number of arcs and nodes as the un-wrapped graph.
     */
    public void testSimulatedGraphSameAsBVSize() throws IOException {
        BVGraph bvGraph = BVGraph.loadMapped("testGraphs/noBlocksUk");
        ImmutableGraphWrapper bvGraphWrapper = new ImmutableGraphWrapper(bvGraph);
        SimulatedGraph simulatedGraph = new SimulatedGraph();

        bvGraphWrapper.iterateAllEdges(edge -> {
            simulatedGraph.addNode(edge.from);
            simulatedGraph.addNode(edge.to);
            simulatedGraph.addEdge(edge);
            return null;
        });

        assertSameSize(bvGraphWrapper,simulatedGraph, bvGraph);

    }

    @Test
    /**
     * Tests that iterating both a real loaded graph and simulated graph clone of it
     * yields the same neighbors.
     */
    public void testSimulatedGraphHaveSameIterators() throws IOException {
        BVGraph bvGraph = BVGraph.loadMapped("testGraphs/noBlocksUk");
        ImmutableGraphWrapper bvGraphWrapper = new ImmutableGraphWrapper(bvGraph);
        SimulatedGraph simulatedGraph = new SimulatedGraph();

        bvGraphWrapper.iterateAllEdges(edge -> {
            simulatedGraph.addNode(edge.from);
            simulatedGraph.addNode(edge.to);
            simulatedGraph.addEdge(edge);
            return null;
        });

        testSameIterators(bvGraphWrapper,simulatedGraph);

    }

    @Test
    public void testSimulatedGraphSameAsBVGraph() throws IOException {
        IGraph simGraph = createSimulatedGraph();
        IGraph bvGraph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/SameAsSimulated"));
        testSameIterators(bvGraph,simGraph);

    }

    @Test
    public void testSimulatedGraphSameSizeAsBVGraph() throws IOException {
        IGraph simGraph = createSimulatedGraph();
        BVGraph bv = BVGraph.load("testGraphs/SameAsSimulated");
        IGraph bvGraph = new ImmutableGraphWrapper(bv);
        assertSameSize(bvGraph,simGraph,bv);
    }

    private void testSameIterators(IGraph bvGraphWrapper, IGraph simulatedGraph){

        for (int i = 0; i < bvGraphWrapper.getNumberOfNodes(); i++) {
            long[] bvNeighborList  = getNeighborList(bvGraphWrapper, i);
            long[] simNeighborList = getNeighborList(simulatedGraph, i);

            assertArrayEquals(bvNeighborList, simNeighborList);
        }

    }

    private void assertSameSize(IGraph bvGraphWrapper, IGraph simulatedGraph, BVGraph bvGraph){


        assertEquals(bvGraphWrapper.getNumberOfNodes(), simulatedGraph.getNumberOfNodes());
        assertEquals(bvGraphWrapper.getNumberOfNodes(), bvGraph.numNodes());

        assertEquals(bvGraphWrapper.getNumberOfArcs(), simulatedGraph.getNumberOfArcs());
        assertEquals(bvGraphWrapper.getNumberOfArcs(), bvGraph.numArcs());
    }

    /**
     * Returns an unsorted list of {@code nodeIndex}'s neighbors in {@code graph}
     * @param graph
     * @param nodeIndex
     * @return
     */
    public long[] getNeighborList(IGraph graph, long nodeIndex) {
        NodeIterator nodeIterator  = graph.getNodeIterator(nodeIndex);
        nodeIterator.nextLong();

        long degree = nodeIterator.outdegree();
        long[] neighborList  = new long[(int)degree];
        LazyLongIterator neighbors  = nodeIterator.successors();

        for(int i=0; i < degree; i++ ) {
            neighborList[i] = neighbors.nextLong();
        }

        return neighborList;
    }


    /**
     * Returns a Simulated graph which is the same as testGraph/SameAsSimulated
     * @return
     */
    public SimulatedGraph createSimulatedGraph(){
        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(29);
        graph.addEdge(new Edge(0,25));
        graph.addEdge(new Edge(0,18));
        graph.addEdge(new Edge(0,27));
        graph.addEdge(new Edge(0,22));
        graph.addEdge(new Edge(0,4));
        graph.addEdge(new Edge(0,6));
        graph.addEdge(new Edge(0,26));
        graph.addEdge(new Edge(0,19));
        graph.addEdge(new Edge(0,2));
        graph.addEdge(new Edge(2,21));
        graph.addEdge(new Edge(2,0));
        graph.addEdge(new Edge(3,20));
        graph.addEdge(new Edge(3,1));
        graph.addEdge(new Edge(3,5));
        graph.addEdge(new Edge(3,19));
        graph.addEdge(new Edge(4,12));
        graph.addEdge(new Edge(4,23));
        graph.addEdge(new Edge(4,28));
        graph.addEdge(new Edge(4,22));
        graph.addEdge(new Edge(5,7));
        graph.addEdge(new Edge(6,10));
        graph.addEdge(new Edge(9,21));
        graph.addEdge(new Edge(9,11));
        graph.addEdge(new Edge(9,8));
        graph.addEdge(new Edge(10,25));
        graph.addEdge(new Edge(10,12));
        graph.addEdge(new Edge(10,10));
        graph.addEdge(new Edge(11,6));
        graph.addEdge(new Edge(12,24));
        graph.addEdge(new Edge(13,11));
        graph.addEdge(new Edge(14,1));
        graph.addEdge(new Edge(14,27));
        graph.addEdge(new Edge(14,3));
        graph.addEdge(new Edge(16,22));
        graph.addEdge(new Edge(16,0));
        graph.addEdge(new Edge(17,15));
        graph.addEdge(new Edge(17,11));
        graph.addEdge(new Edge(19,22));
        graph.addEdge(new Edge(19,9));
        graph.addEdge(new Edge(19,11));
        graph.addEdge(new Edge(20,14));
        graph.addEdge(new Edge(20,0));
        graph.addEdge(new Edge(20,21));
        graph.addEdge(new Edge(20,17));
        graph.addEdge(new Edge(20,23));
        graph.addEdge(new Edge(20,11));
        graph.addEdge(new Edge(21,10));
        graph.addEdge(new Edge(22,22));
        graph.addEdge(new Edge(23,29));
        graph.addEdge(new Edge(23,23));
        graph.addEdge(new Edge(23,10));
        graph.addEdge(new Edge(24,24));
        graph.addEdge(new Edge(25,19));
        graph.addEdge(new Edge(27,1));
        graph.addEdge(new Edge(27,9));
        graph.addEdge(new Edge(28,19));
        graph.addEdge(new Edge(28,13));
        graph.addEdge(new Edge(28,21));
        graph.addEdge(new Edge(29,5));
        graph.addEdge(new Edge(29,16));
        return graph;
    }

}
