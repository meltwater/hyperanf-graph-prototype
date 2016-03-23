package se.meltwater.test.graph;

import it.unimi.dsi.big.webgraph.BVGraph;
import org.junit.Test;
import se.meltwater.graph.Edge;
import se.meltwater.graph.ImmutableGraphWrapper;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class TestImmutableGraphWrapper {

    ImmutableGraphWrapper graph;

    @Test
    public void testWrapperUnchangedOnExistingEdge() throws IOException {

        setupGraph();

        assertFalse(graph.addEdge(new Edge(0,27))); //Should already exist

    }

    @Test
    public void testEdgeAdded() throws IOException{
        setupGraph();

        Edge toAdd = new Edge(0,23);
        assertTrue(graph.addEdge(toAdd));

        Boolean foundEdge = graph.iterateAllEdges((Edge e) -> {
            if(e.equals(toAdd)){
                return true;
            }
            return null;
        });
        assertNotNull(foundEdge);

    }

    @Test
    public void testNumNodesIncrease() throws IOException{
        for (int i = 0; i < 100 ; i++) {
            setupGraph();
            Edge edge = randomEdgeToAdd();
            assertTrue(graph.addEdge(edge));
            assertEquals(Math.max(edge.to,edge.from)+1,graph.getNumberOfNodes());
        }
    }

    @Test
    public void testTranspose() throws IOException {
        setupGraph();
        assertNotEquals(graph, graph.transpose());
        assertEquals(graph, graph.transpose().transpose());
    }

    public Edge randomEdgeToAdd(){
        long numNodes = graph.getNumberOfNodes();
        long nodeLessThanNumNodes = ThreadLocalRandom.current().nextLong(numNodes);
        long nodeMoreThanNumNodes = ThreadLocalRandom.current().nextLong(numNodes)+numNodes;
        boolean firstGreaterThanNumNodes = ThreadLocalRandom.current().nextBoolean();
        return new Edge(firstGreaterThanNumNodes ? nodeMoreThanNumNodes : nodeLessThanNumNodes,
                        firstGreaterThanNumNodes ? nodeLessThanNumNodes : nodeMoreThanNumNodes);
    }

    private void setupGraph() throws IOException {
        graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/SameAsSimulated"));
    }

}
