package se.meltwater.test.graph;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.junit.Test;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraphWrapper;
import it.unimi.dsi.big.webgraph.SimulatedGraph;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class TestDifferentGraphs {

    @Test
    public void testSimulatedEqualsBVGraph() throws IOException {
        MutableGraph simGraph = createSimulatedGraph();
        MutableGraph bvGraph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/SameAsSimulated"));
        assertEquals(simGraph, bvGraph);
    }

    @Test
    public void testSimulatedGraphSameAsBVGraph() throws IOException {
        MutableGraph simGraph = createSimulatedGraph();
        MutableGraph bvGraph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/SameAsSimulated"));
        testSameIterators(bvGraph,simGraph);

    }

    @Test
    public void testSimulatedGraphSameSizeAsBVGraph() throws IOException {
        MutableGraph simGraph = createSimulatedGraph();
        BVGraph bv = BVGraph.load("testGraphs/SameAsSimulated");
        MutableGraph bvGraph = new ImmutableGraphWrapper(bv);
        assertSameSize(bvGraph,simGraph,bv);
    }

    /**
     * Tests that both a real loaded graph and a similated graph clone of it
     * have the same number of arcs and nodes. Also checks that ImmutableGraphWrapper
     * have the same number of arcs and nodes as the un-wrapped graph.
     */
    @Test
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

    /**
     * Tests that iterating both a real loaded graph and simulated graph clone of it
     * yields the same neighbors.
     */
    @Test
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


    private void testSameIterators(MutableGraph bvGraphWrapper, MutableGraph simulatedGraph){

        for (int i = 0; i < bvGraphWrapper.numNodes(); i++) {
            long[] bvNeighborList  = getNeighborList(bvGraphWrapper, i);
            long[] simNeighborList = getNeighborList(simulatedGraph, i);

            assertArrayEquals(bvNeighborList, simNeighborList);
        }

    }

    private void assertSameSize(MutableGraph bvGraphWrapper, MutableGraph simulatedGraph, BVGraph bvGraph){


        assertEquals(bvGraphWrapper.numNodes(), simulatedGraph.numNodes());
        assertEquals(bvGraphWrapper.numNodes(), bvGraph.numNodes());

        assertEquals(bvGraphWrapper.numArcs(), simulatedGraph.numArcs());
        assertEquals(bvGraphWrapper.numArcs(), bvGraph.numArcs());
    }

    /**
     * Returns an unsorted list of {@code nodeIndex}'s neighbors in {@code graph}
     * @param graph
     * @param nodeIndex
     * @return
     */
    public long[] getNeighborList(MutableGraph graph, long nodeIndex) {
        NodeIterator nodeIterator  = graph.nodeIterator(nodeIndex);
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
        graph.addEdge(new Edge(23,29));
        graph.addEdge(new Edge(23,10));
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
