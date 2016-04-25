package se.meltwater.test.graph;

import com.google.common.collect.Lists;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.junit.Test;
import se.meltwater.graph.Edge;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.TraverseGraph;
import se.meltwater.test.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class TestTraverseGraph {

    private TraverseGraph graph;
    private int numNodes;
    private static final int maxNumNodes = 1000;
    private Random rand;

    private void setupGraph() throws IOException {
        graph = new TraverseGraph(new Edge[0]);
        rand = new Random();
    }

    /**
     * Tests that the added edges to the graph exists, the old ones remain, and no
     * edges are created from nothing.
     * @throws IOException
     */
    @Test
    public void testAddedEdgesCorrect() throws IOException {
        setupGraph();

        for(int i = 0; i< 100; i++) {

            numNodes = rand.nextInt(maxNumNodes)+1;
            ArrayList<Edge> edgesBefore = getEdgesBefore();

            ArrayList<Edge> newEdges = generateNewEdges();
            removeEdgesAppearingInGraph(edgesBefore, newEdges);
            assertEquals(0,newEdges.size());
            assertEquals(0,edgesBefore.size());

        }

    }

    private void removeEdgesAppearingInGraph(ArrayList<Edge> edgesBefore, ArrayList<Edge> newEdges) {
        NodeIterator it = graph.nodeIterator();
        while (it.hasNext()){
            long node = it.nextLong();
            long neigh;
            LazyLongIterator it2 = it.successors();
            while ((neigh = it2.nextLong()) != -1) {
                Edge e = new Edge(node, neigh);
                assertTrue(newEdges.contains(e) || edgesBefore.contains(e));
                while (newEdges.remove(e)) ;
                while (edgesBefore.remove(e));
            }
        }
    }

    private ArrayList<Edge> generateNewEdges() {
        int numEdges = maxNumNodes;
        ArrayList<Edge> edges = Lists.newArrayList(TestUtils.generateEdges(numNodes, numEdges));

        Edge[] edgeArr = edges.toArray(new Edge[edges.size()]);
        graph.addEdges(edgeArr);
        return edges;
    }

    private ArrayList<Edge> getEdgesBefore() {
        ArrayList<Edge> edgesBefore = new ArrayList<>((int)graph.numNodes());
        NodeIterator it = graph.nodeIterator();
        while (it.hasNext()){
            long node = it.nextLong();
            long neigh;
            LazyLongIterator it2 = it.successors();
            while ((neigh = it2.nextLong()) != -1)
                edgesBefore.add(new Edge(node,neigh));
        }
        return edgesBefore;
    }
}
