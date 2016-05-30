package it.unimi.dsi.big.webgraph.graph;

import com.google.common.collect.Lists;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.TestUtils;
import org.junit.Test;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.ImmutableGraphWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class ImmutableGraphWrapperTest {

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
            assertEquals(Math.max(edge.to,edge.from)+1,graph.numNodes());
        }
    }

    @Test
    public void testTranspose() throws IOException {
        setupGraph();
        assertNotEquals(graph, graph.transpose());
        assertEquals(graph, graph.transpose().transpose());
    }

    public Edge randomEdgeToAdd(){
        long numNodes = graph.numNodes();
        long nodeLessThanNumNodes = ThreadLocalRandom.current().nextLong(numNodes);
        long nodeMoreThanNumNodes = ThreadLocalRandom.current().nextLong(numNodes)+numNodes;
        boolean firstGreaterThanNumNodes = ThreadLocalRandom.current().nextBoolean();
        return new Edge(firstGreaterThanNumNodes ? nodeMoreThanNumNodes : nodeLessThanNumNodes,
                        firstGreaterThanNumNodes ? nodeLessThanNumNodes : nodeMoreThanNumNodes);
    }

    private void setupGraph() throws IOException {
        graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/SameAsSimulated"));
    }



    /**
     * Tests that the added edges to the graph exists, the old ones remain, and no
     * edges are created from nothing.
     * @throws IOException
     */
    @Test
    public void testAddedEdgesCorrect() throws IOException {
        ImmutableGraphWrapper graph = new ImmutableGraphWrapper(BVGraph.loadMapped("testGraphs/SameAsSimulated"));
        int numNodes;
        int maxNodes = 100;
        Random rand = new Random();

        for(int i = 0; i< 100; i++) {

            numNodes = rand.nextInt(maxNodes)+1;
            ArrayList<Edge> edgesBefore = getEdgesBefore(graph);

            ArrayList<Edge> newEdges = generateNewEdges(numNodes, maxNodes,graph);
            removeEdgesAppearingInGraph(edgesBefore, newEdges,graph);
            assertEquals(0,newEdges.size());
            assertEquals(0,edgesBefore.size());

        }

    }

    private void removeEdgesAppearingInGraph(ArrayList<Edge> edgesBefore, ArrayList<Edge> newEdges, ImmutableGraphWrapper graph) {
        graph.iterateAllEdges(e ->{
            assertTrue(newEdges.contains(e) || edgesBefore.contains(e));
            while (newEdges.remove(e)) ;
            while (edgesBefore.remove(e));
            return null;
        });

    }

    private ArrayList<Edge> generateNewEdges(int numNodes, int maxNodes, ImmutableGraphWrapper graph) {
        int numEdges = maxNodes;
        ArrayList<Edge> edges = Lists.newArrayList(TestUtils.generateEdges(numNodes, numEdges));

        Edge[] edgeArr = edges.toArray(new Edge[edges.size()]);
        graph.addEdges(edgeArr);
        return edges;
    }

    private ArrayList<Edge> getEdgesBefore(ImmutableGraphWrapper graph) {
        ArrayList<Edge> edgesBefore = new ArrayList<>();
        graph.iterateAllEdges(e -> {
            edgesBefore.add(e);
            return null;
        });
        return edgesBefore;
    }

}
