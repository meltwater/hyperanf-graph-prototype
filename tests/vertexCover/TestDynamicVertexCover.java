package vertexCover;

import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.graph.Edge;
import se.meltwater.graph.Node;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.vertexcover.DynamicVertexCover;

/**
 * Created by johan on 2016-02-29.
 */
public class TestDynamicVertexCover {



    @Test
    public void testInsertions() {
        Node[] nodes = {new Node(0), new Node(1), new Node(2)};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    public void testDeletionsInMaximal() {
        Node[] nodes = {new Node(0), new Node(1), new Node(2)};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        graph.removeEdge(edges[0]);
        dvc.deleteEdge(edges[0]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    public void testDeletionsOutsideMaximal() {
        Node[] nodes = {new Node(0), new Node(1), new Node(2)};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        graph.removeEdge(edges[1]);
        dvc.deleteEdge(edges[1]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    public void testDeleteAllEdges() {
        Node[] nodes = {new Node(0), new Node(1), new Node(2)};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = setupSGraph(nodes, edges);
        DynamicVertexCover dvc = setupDVC(graph);

        for(Edge edge : edges ) {
            graph.removeEdge(edge);
            dvc.deleteEdge(edge);
        }

        assertTrue(isVertexCover(graph, dvc));
        assertTrue(dvc.getVertexCoverSize() == 0);
        assertTrue(dvc.getMaximalMatchingSize() == 0);
    }

    public SimulatedGraph setupSGraph(Node[] nodes, Edge[] edges) {
        SimulatedGraph graph = new SimulatedGraph();

        for(Node node : nodes) {
            graph.addNode(node);
        }

        for(Edge edge : edges) {
            graph.addEdge(edge);
        }

        return graph;
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
