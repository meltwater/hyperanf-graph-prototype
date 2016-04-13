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
    private static final int maxNumNodes = 10;
    private Random rand;

    private void setupGraph() throws IOException {
        graph = new TraverseGraph(new Edge[0]);
        rand = new Random();
    }

    @Test
    public void testAddedEdgesCorrect() throws IOException {
        setupGraph();

        //graph.addEdges(new Edge[]{new Edge(0,0), new Edge(1,1)});

        for(int i = 0; i< 100; i++) {

            numNodes = rand.nextInt(maxNumNodes)+1;
            ArrayList<Edge> edgesBefore = new ArrayList<>((int)graph.numNodes());
            NodeIterator it = graph.nodeIterator();
            while (it.hasNext()){
                long node = it.nextLong();
                long neigh;
                LazyLongIterator it2 = it.successors();
                while ((neigh = it2.nextLong()) != -1)
                    edgesBefore.add(new Edge(node,neigh));
            }
            ArrayList<Edge> edgesBeforeClone = (ArrayList<Edge>) edgesBefore.clone();

            int numEdges = 2;
            ArrayList<Edge> edges = Lists.newArrayList(TestUtils.generateEdges(numNodes, numEdges));

            Edge[] edgeArr = edges.toArray(new Edge[edges.size()]);
            graph.addEdges(edgeArr);
            it = graph.nodeIterator();
            while (it.hasNext()){
                long node = it.nextLong();
                long neigh;
                LazyLongIterator it2 = it.successors();
                while ((neigh = it2.nextLong()) != -1) {
                    Edge e = new Edge(node, neigh);
                    assertTrue(edges.contains(e) || edgesBefore.contains(e));
                    while (edges.remove(e)) ;
                    while (edgesBefore.remove(e));
                }
            }
            assertEquals(0,edges.size());
            assertEquals(0,edgesBefore.size());

        }

    }
}
