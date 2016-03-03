package se.meltwater.test.vertexCover;

import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;

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

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
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
        Edge[] edges = TestUtils.generateEdges(n, m);

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, new Edge[0]);
        DynamicVertexCover dvc = setupDVC(graph);

        for(int i = 0; i < edges.length; i++) {
            graph.addEdge(edges[i]);
            dvc.insertEdge(edges[i]);
            assertTrue(isVertexCover(graph,dvc));
        }

        for(int i = 0; i < edges.length; i++) {
            graph.removeEdge(edges[i]);
            dvc.deleteEdge(edges[i]);
            boolean isVC = isVertexCover(graph,dvc);
            if(!isVC) {
                System.out.println("here");
            }

            assertTrue(isVC);
        }
    }

    @Test
    public void testDeletionsInMaximal() {
        long[] nodes = {0, 1, 2, 3};
        Edge[] edges = {new Edge(nodes[0], nodes[2]),
                new Edge(nodes[1], nodes[2])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        graph.removeEdge(edges[0]);
        dvc.deleteEdge(edges[0]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    public void testDeletionsOutsideMaximal() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        graph.removeEdge(edges[1]);
        dvc.deleteEdge(edges[1]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    public void testDeleteAllEdges() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        for(Edge edge : edges ) {
            graph.removeEdge(edge);
            dvc.deleteEdge(edge);
        }

        assertTrue(isVertexCover(graph, dvc));
        assertTrue(dvc.getVertexCoverSize() == 0);
        assertTrue(dvc.getMaximalMatchingSize() == 0);
    }

    public DynamicVertexCover setupDVC(SimulatedGraph graph) {
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        for(Edge edge : graph.edges) {
            dvc.insertEdge(edge);
        }

        return dvc;
    }

    public boolean isVertexCover(SimulatedGraph graph, DynamicVertexCover dvc) {
        for(Edge edge : graph.edges) {
            if(!(dvc.isInVertexCover(edge.from) || dvc.isInVertexCover(edge.to))) {
                return false;
            }
        }

        return true;
    }
}
