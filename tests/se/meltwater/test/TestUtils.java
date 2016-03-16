package se.meltwater.test;

import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

/**
 * Functions that are useful across several test classes.
 */
public class TestUtils {

    /**
     * Generates a edge-graph of {@code n} nodes and {@code m} random edges between them.
     * @param n
     * @param m
     * @return
     */
    public static Edge[] generateEdges(int n, int m) {
        HashMap<Long, Long> edgesMap = new HashMap<>();
        ArrayList<Edge> edges = new ArrayList<>();

        for(int i = 0; i < m; i++) {
            Random rand = new Random();
            long to   = rand.nextInt(n);
            long from = rand.nextInt(n);
            if(to != from) {
                edgesMap.put(to, from);
            } else {
                i--; //Try again to add an edge
            }
        }

        for(Map.Entry<Long, Long> edge : edgesMap.entrySet()) {
            edges.add(new Edge(edge.getKey(), edge.getValue()));
        }

        Edge[] edgeArray = new Edge[edges.size()];
        return edges.toArray(edgeArray);
    }

    /**
     * Creates a Simulated Graph containing {@code nodes} and {@code edges}
     * @param nodes
     * @param edges
     * @return
     */
    public static SimulatedGraph setupSGraph(long[] nodes, Edge[] edges) {
        SimulatedGraph graph = new SimulatedGraph();

        for(long node : nodes) {
            graph.addNode(node);
        }

        for(Edge edge : edges) {
            graph.addEdge(edge);
        }

        return graph;
    }

    /**
     * Randomly generates a graph consisting of at max {@code maxNumNodes} nodes
     * and 2^{@code maxNumNodes} edges.
     * At least 2 nodes and 1 edge will be generated.
     * @param maxNumNodes
     * @return
     */
    public static SimulatedGraph genRandomGraph(int maxNumNodes){
        Random rand = new Random();
        int n = maxNumNodes > 2 ? rand.nextInt(maxNumNodes - 2) + 2 : 2;
        int m = rand.nextInt((int)Math.pow(n, 2) - 1) + 1;

        long[] nodes = LongStream.rangeClosed(0, n).toArray();
        Edge[] edges = generateEdges(n, m);

        return setupSGraph(nodes, edges);
    }
}
