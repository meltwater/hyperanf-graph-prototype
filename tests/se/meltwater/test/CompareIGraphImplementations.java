package se.meltwater.test;

import it.unimi.dsi.big.webgraph.BVGraph;
import org.junit.Test;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class CompareIGraphImplementations {

    private IGraph simGraph;
    private IGraph loadedGraph;

    @Test
    public void compareIGraphImplementations() throws IOException {
        IGraph simGraph = createSimulatedGraph();
        IGraph loadedGraph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/SameAsSimulated"));
    }

    private void printRandomSimulatedAndArcListGraph(int numNodes, int numEdges){
        Edge[] edges = TestUtils.generateEdges(numNodes,numEdges);
        Arrays.sort(edges,(Edge a, Edge b) -> (int)(a.from-b.from));
        System.out.println("        graph.addNode(" + (numNodes+1) + ");");
        for (Edge edge: edges) {
            System.out.println("        graph.addEdge(new Edge(" + edge.from + "," + edge.to + "));");
        }
        for (Edge edge: edges) {
            System.out.println(edge.from + " " + edge.to);
        }
    }

    public SimulatedGraph createSimulatedGraph(){
        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(30);
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
        graph.addEdge(new Edge(10,10));
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
        graph.addEdge(new Edge(22,22));
        graph.addEdge(new Edge(23,29));
        graph.addEdge(new Edge(23,23));
        graph.addEdge(new Edge(23,10));
        graph.addEdge(new Edge(24,24));
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
