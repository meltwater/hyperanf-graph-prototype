package se.meltwater.test;

import javafx.util.Pair;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import static org.junit.Assert.assertTrue;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
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
        Edge[] edges = new Edge[m];

        Random rand = new Random();
        for(int i = 0; i < m; i++) {
            long to   = rand.nextInt(n);
            long from = rand.nextInt(n);
            edges[i] = new Edge(to, from);
        }

        return edges;
    }

    public static Edge generateEdge(long minFrom, long maxFrom, long minTo, long maxTo) {
        /* In a world where Java would have a bounded nextLong method in their
           Random class this would be new Random().nextLong(n);
         */
        long from = ThreadLocalRandom.current().nextLong(maxFrom - minFrom) + minFrom;
        long to   = ThreadLocalRandom.current().nextLong(maxTo - minTo) + minTo;

        return new Edge(from, to);
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

    public static long[] generateRandomNodes(long numNodesInGraph, int maxGeneratedNodes, int minGeneratedNodes){
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        int num = rand.nextInt(maxGeneratedNodes-minGeneratedNodes)+minGeneratedNodes;
        long[] gen = new long[num];
        for (int i = 0; i < num ; i++) {
            gen[i] = rand.nextLong(numNodesInGraph);
        }
        return gen;
    }

    public static int[] generateRandomIntNodes(int numNodesInGraph, int maxGeneratedNodes, int minGeneratedNodes){
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        int num = rand.nextInt(maxGeneratedNodes-minGeneratedNodes)+minGeneratedNodes;
        int[] gen = new int[num];
        for (int i = 0; i < num ; i++) {
            gen[i] = rand.nextInt(numNodesInGraph);
        }
        return gen;
    }

    /**
     *
     * @param runnable
     */
    public static void assertGivesException(Runnable runnable) {
        boolean hadException = false;
        try {
            runnable.run();
        } catch (Exception e) {
            hadException = true;
        }
        assertTrue(hadException);
    }

}
