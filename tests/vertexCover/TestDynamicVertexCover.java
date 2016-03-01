package vertexCover;

import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

/**
 * Created by johan on 2016-02-29.
 */
public class TestDynamicVertexCover {



    @Test
    public void testInsertions() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test //OPA QUICKCHECK STYLE
    public void testRandomInsertions() {
        final int maxNumNodes = 500;
        Random rand = new Random();
        int n = rand.nextInt(maxNumNodes);
        int m = rand.nextInt((int)Math.pow(n, 2));

        long[] nodes = LongStream.rangeClosed(0, n).toArray();
        Edge[] edges = generateEdges(n, m);

        SimulatedGraph graph = setupSGraph(nodes, new Edge[0]);
        DynamicVertexCover dvc = setupDVC(graph);

        for(int i = 0; i < edges.length; i++) {
            graph.addEdge(edges[i]);
            dvc.insertEdge(edges[i]);
            assertTrue(isVertexCover(graph,dvc));
        }

        for(int i = 0; i < edges.length; i++) {
            graph.deleteEdge(edges[i]);
            dvc.deleteEdge(edges[i]);
            assertTrue(isVertexCover(graph,dvc));
        }
    }

    private Edge[] generateEdges(int n, int m) {
        HashMap<Long, Long> edgesMap = new HashMap<>();
        ArrayList<Edge> edges = new ArrayList<>();

        for(int i = 0; i < m; i++) {
            Random rand = new Random();
            long to   = rand.nextInt(n);
            long from = rand.nextInt(n);
            if(to != from) {
                edgesMap.put(to, from);
            }
        }

        for(Map.Entry<Long, Long> edge : edgesMap.entrySet()) {
            edges.add(new Edge(edge.getKey(), edge.getValue()));
        }

        Edge[] edgeArray = new Edge[edges.size()];
        return edges.toArray(edgeArray);
    }

    @Test
    public void testDeletionsInMaximal() {
        long[] nodes = {0, 1, 2, 3};
        Edge[] edges = {new Edge(nodes[0], nodes[2]),
                new Edge(nodes[1], nodes[2])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        graph.deleteEdge(edges[0]);
        dvc.deleteEdge(edges[0]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    public void testDeletionsOutsideMaximal() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        graph.deleteEdge(edges[1]);
        dvc.deleteEdge(edges[1]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    public void testDeleteAllEdges() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        for(Edge edge : edges ) {
            graph.deleteEdge(edge);
            dvc.deleteEdge(edge);
        }

        assertTrue(isVertexCover(graph, dvc));
        assertTrue(dvc.getVertexCoverSize() == 0);
        assertTrue(dvc.getMaximalMatchingSize() == 0);
    }

    public SimulatedGraph setupSGraph(long[] nodes, Edge[] edges) {
        SimulatedGraph graph = new SimulatedGraph();

        for(long node : nodes) {
            graph.addNode(node);
        }

        for(Edge edge : edges) {
            graph.addEdge(edge);
        }

        return graph;
    }

    public DynamicVertexCover setupDVC(SimulatedGraph graph) {
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        long n = graph.getNumberOfNodes();

        for(int i = 0; i < n; i++) {
            graph.setNodeIterator(i);
            long outdegree = graph.getOutdegree();

            while(outdegree != 0) {
                long neighbor = graph.getNextNeighbor();
                dvc.insertEdge(new Edge(i, neighbor));
                outdegree--;
            }
        }


        return dvc;
    }

    public boolean isVertexCover(SimulatedGraph graph, DynamicVertexCover dvc) {
        long n = graph.getNumberOfNodes();

        for(int i = 0; i < n; i++) {
            graph.setNodeIterator(i);
            long outdegree = graph.getOutdegree();

            while(outdegree != 0) {
                long neighbor = graph.getNextNeighbor();
                if(!(dvc.isInVertexCover(i) || dvc.isInVertexCover(neighbor))) {
                    return false;
                }
                outdegree--;
            }
        }


        return true;
    }
}
