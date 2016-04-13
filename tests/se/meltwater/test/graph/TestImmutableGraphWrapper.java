package se.meltwater.test.graph;

import com.google.common.collect.Lists;
import it.unimi.dsi.big.webgraph.BVGraph;
import org.junit.Test;
import se.meltwater.graph.Edge;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.test.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
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

    @Test
    public void testAddedEdgesCorrect() throws IOException {
        setupGraph();

        for(int i = 0; i< 10; i++) {

            int numEdges = 500;
            ArrayList<Edge> edges = Lists.newArrayList(TestUtils.generateEdges((int) graph.getNumberOfNodes(), numEdges));

            Edge[] edgeArr = edges.toArray(new Edge[edges.size()]);
            graph.addEdges(edgeArr);
            graph.iterateAllEdges((Edge e) -> {
                while (edges.remove(e));
                return null;
            });
            assertEquals(0,edges.size());

        }

    }

}
