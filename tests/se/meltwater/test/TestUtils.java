package se.meltwater.test;

import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

/**
 * Created by simon on 2016-03-01.
 */
public class TestUtils {

    public static Edge[] generateEdges(int n, int m) {
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

    public static SimulatedGraph genRandomGraph(int maxNumNodes){

        Random rand = new Random();
        int n = rand.nextInt(maxNumNodes);
        int m = rand.nextInt((int)Math.pow(n, 2));

        long[] nodes = LongStream.rangeClosed(0, n).toArray();
        Edge[] edges = generateEdges(n, m);

        return setupSGraph(nodes, edges);
    }

}
