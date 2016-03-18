package se.meltwater.test.graph;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class for testing that {@link SimulatedGraph} works
 * gives the same results as {@link ImmutableGraphWrapper}
 */
public class TestSimulatedGraph {

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

        assertTrue(bvGraphWrapper.getNumberOfNodes() == simulatedGraph.getNumberOfNodes());
        assertTrue(bvGraphWrapper.getNumberOfNodes() == bvGraph.numNodes());

        assertTrue(bvGraphWrapper.getNumberOfArcs() == simulatedGraph.getNumberOfArcs());
        assertTrue(bvGraphWrapper.getNumberOfArcs() == bvGraph.numArcs());
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

        for (int i = 0; i < bvGraphWrapper.getNumberOfNodes(); i++) {
            ArrayList<Long> bvNeighborList  = getNeighborList(bvGraphWrapper, i);
            ArrayList<Long> simNeighborList = getNeighborList(simulatedGraph, i);

            assertSameNeighbors(bvNeighborList, simNeighborList);
        }
    }

    /**
     * Returns an unsorted list of {@code nodeIndex}'s neighbors in {@code graph}
     * @param graph
     * @param nodeIndex
     * @return
     */
    public ArrayList<Long> getNeighborList(IGraph graph, long nodeIndex) {
        NodeIterator nodeIterator  = graph.getNodeIterator(nodeIndex);
        nodeIterator.nextLong();

        ArrayList<Long> neighborList  = new ArrayList<>();
        LazyLongIterator neighbors  = nodeIterator.successors();
        long degree = nodeIterator.outdegree();

        while(degree-- > 0 ) {
            neighborList.add(neighbors.nextLong());
        }

        return neighborList;
    }

    /**
     * Asserts that both lists include the same elements
     * @param neighborList1
     * @param neighborList2
     */
    private void assertSameNeighbors(ArrayList<Long> neighborList1, ArrayList<Long> neighborList2) {
        assertTrue(neighborList1.size() == neighborList2.size());
        assertTrue(neighborList1.containsAll(neighborList2));
    }
}
