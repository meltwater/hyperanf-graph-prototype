package se.meltwater.test.history;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.UnionImmutableGraph;
import javafx.util.Pair;
import org.junit.Test;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class TestEffectPropagation {

    /* For some tests its necessary to have a static seed to
     * make sure we always get the same result from HyperBoll */
    final long fixedSeed = 8516942932596937874L;

    final float epsilon = 0.1f;

    final int maxNumNodes = 5;
    int log2m;
    int h;

    @Test
    public void testDANFSameAsHyperBallOnImmutableGraphs() throws IOException, InterruptedException {
        Random rand = new Random();
        log2m = rand.nextInt(7)+4;
        h = rand.nextInt(5)+1;

        BVGraph g1 = BVGraph.load("testGraphs/noBlocksUk");
        BVGraph g2 = BVGraph.load("testGraphs/wordassociationNoBlocks");

        Edge[] additionalEdges = new Edge[(int)g2.numArcs()]; //wordassociation only contains 10k nodes
        AtomicInteger i = new AtomicInteger(0);
        new ImmutableGraphWrapper(g2).iterateAllEdges((Edge e) -> {
            additionalEdges[i.getAndIncrement()] = e;
            return null;
        });

        IGraph merged = new ImmutableGraphWrapper(new UnionImmutableGraph(g1,g2));
        ImmutableGraphWrapper g1g = new ImmutableGraphWrapper(g1);
        testDANFSameAsHyperBall(g1g,additionalEdges,merged);
        g1g.close();

    }

    /**
     *
     *
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDANFSameAsHyperBallOnSimulatedGraphs() throws IOException, InterruptedException {

        Random rand = new Random();
        for (int i = 0; i < 100 ; i++) {

            log2m = rand.nextInt(7)+4;
            h = rand.nextInt(5)+1;

            SimulatedGraph g1 = TestUtils.genRandomGraph(maxNumNodes);
            int numExtraNodes = rand.nextInt(maxNumNodes)+1;
            int numEdges = rand.nextInt((int)Math.pow(numExtraNodes,1));
            Edge[] additionalEdges = TestUtils.generateEdges(numExtraNodes,numEdges);

            SimulatedGraph mergedGraph = (SimulatedGraph) g1.clone();
            mergedGraph.addEdges(additionalEdges);
            testDANFSameAsHyperBall(g1,additionalEdges,mergedGraph);

        }
    }

    private void testDANFSameAsHyperBall(IGraph g1, Edge[] additionalEdges, IGraph mergedGraph) throws IOException, InterruptedException {

        IGraph clone = (IGraph) ((SimulatedGraph)g1).clone();
        long seed = Util.randomSeed();
        Pair<DANF,HyperBoll> pair = TestUtils.runHyperBall(g1,new DynamicVertexCover(g1),h,log2m, seed);
        DANF danf = pair.getKey();

        danf.addEdges(additionalEdges);

        HyperLolLolCounterArray hll = TestUtils.runHyperBall(mergedGraph,h,log2m,seed).getCounter();
        for (long i = 0; i < mergedGraph.getNumberOfNodes() ; i++) {
            assertEquals("Node " + i,hll.count(i),danf.count(i,h),epsilon);
        }

    }

    @Test
    public void testStaticRandomGraph3() throws IOException, InterruptedException {
      //  while(true) {
            long seed = 1607655446946170955L;
            h = 3;
            log2m = 8;

            SimulatedGraph graph = new SimulatedGraph();
            graph.addNode(4);
            graph.addEdges(new Edge(0, 3), new Edge(2, 1), new Edge(2, 3), new Edge(3, 0), new Edge(3, 2));

            SimulatedGraph mergedGraph = (SimulatedGraph) graph.clone();
            mergedGraph.addEdges(new Edge(0, 2));

            DynamicVertexCover vc = new DynamicVertexCover(graph);
            DANF danf = TestUtils.runHyperBall(graph, vc, h, log2m, seed).getKey();

            danf.addEdges(new Edge(0, 2));

            HyperLolLolCounterArray hll = TestUtils.runHyperBall(mergedGraph, h, log2m, seed).getCounter();
            for (long i = 0; i < mergedGraph.getNumberOfNodes(); i++) {
                assertEquals("Node " + i, hll.count(i), danf.count(i, h), epsilon);
            }
        //}
    }

    @Test
    public void testStaticRandomGraph2() throws IOException, InterruptedException {
        long seed = 4405387622461906011L;
        h = 3;
        log2m = 8;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(3);
        graph.addEdges(new Edge(1,1),new Edge(2,1));

        SimulatedGraph mergedGraph = (SimulatedGraph) graph.clone();
        mergedGraph.addEdges(new Edge(0,2));

        DynamicVertexCover vc = new DynamicVertexCover(graph);
        DANF danf = TestUtils.runHyperBall(graph, vc, h, log2m, seed).getKey();

        danf.addEdges(new Edge(0, 2));

        HyperLolLolCounterArray hll = TestUtils.runHyperBall(mergedGraph, h, log2m, seed).getCounter();
        for (long i = 0; i < mergedGraph.getNumberOfNodes(); i++) {
            assertEquals("Node " + i, hll.count(i), danf.count(i, h), epsilon);
        }

    }

    @Test
    public void testStaticRandomGraph() throws IOException, InterruptedException {

        h = 3;
        log2m = 6;
        long seed = -6861637300863366465L;


        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);
        graph.addEdges();
        SimulatedGraph mergedGraph = (SimulatedGraph) graph.clone();
        mergedGraph.addEdges(new Edge(0, 0), new Edge(0, 1), new Edge(1, 3));

        DynamicVertexCover vc = new DynamicVertexCover(graph);
        DANF danf = TestUtils.runHyperBall(graph, vc, h, log2m, seed).getKey();

        danf.addEdges(new Edge(0, 1), new Edge(1, 3), new Edge(0,0));

        HyperLolLolCounterArray hll = TestUtils.runHyperBall(mergedGraph, h, log2m, seed).getCounter();
        for (long i = 0; i < mergedGraph.getNumberOfNodes(); i++) {
            assertEquals("Node " + i, hll.count(i), danf.count(i, h), epsilon);
        }

    }

    /**
     * 0 -> N1
     * @throws IOException
     */
    @Test
    public void testOnlyIncomingEdgeSingleNewNode() throws IOException, InterruptedException {
        h = 4;
        log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h, log2m, 0);

        danf.addEdges(new Edge(0, 1));
        assertArrayEquals(new double[]{2.0,2.0,2.0,2.0}, danf.count(0), epsilon);
    }

    /**
     * 0 -> N1
     * @throws IOException
     */
    @Test
    public void testOnlyIncomingEdgeSingleNewNodeTwice() throws IOException, InterruptedException {
        h = 4;
        log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h, log2m, 0);

        danf.addEdges(new Edge(0, 1),new Edge(0,1));

        assertArrayEquals(new double[]{2.0,2.0,2.0,2.0}, danf.count(0), epsilon);
    }



    /**
     * <pre>{@code
     * 0 -> 2 -> N3
     *      ^
     *      |
     *      1
     * }</pre>
     */
    @Test
    public void testOnlyIncomingEdgesSingleNewNode() throws InterruptedException, IOException {
        h = 4;
        log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h, log2m, 2, new Edge(0,2), new Edge(1,2));

        assertEquals(2.0f, danf.count(0, h), epsilon);
        assertEquals(2.0f, danf.count(1, h), epsilon);
        assertEquals(1.0f, danf.count(2, h), epsilon);

        final int newNode = 3;
        danf.addEdges(new Edge(2, newNode));

        assertArrayEquals(new double[]{2.0,3.0,3.0,3.0}, danf.count(0), epsilon);
        assertEquals(3.0f, danf.count(1, h), epsilon);
        assertArrayEquals(new double[]{2.0,2.0,2.0,2.0}, danf.count(2), epsilon);
        assertEquals(1.0f, danf.count(3, h), epsilon);
    }


    /**
     * <pre>{@code
     *      1
     *      |
     *      v
     * 0 -> N4 -> 2
     *      |
     *      v
     *      3
     * }</pre>
     */
    @Test
    public void testIncomingAndOutgoingEdgesSingleNewNode() throws IOException, InterruptedException {
        h = 4;
        log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h, log2m, 3);

        final int newNode = 4;

        danf.addEdges(new Edge(0, newNode), new Edge(1, newNode),
                      new Edge(newNode, 2), new Edge(newNode, 3));

        assertArrayEquals(new double[]{2.0,4.0,4.0,4.0}, danf.count(0), epsilon);
        assertEquals(4.0f, danf.count(1, h), epsilon);
        assertEquals(1.0f, danf.count(2, h), epsilon);
        assertEquals(1.0f, danf.count(3, h), epsilon);
        assertArrayEquals(new double[]{3.0,3.0,3.0,3.0}, danf.count(newNode), epsilon);
    }

    /**
     * <pre>{@code
     * 0 -> 2 -> N4
     * 1 -> 3 -> N5
     * }</pre>
     */
    @Test
    public void testOnlyIncomingEdgesMultipleNewNodes() throws IOException, InterruptedException {
        h = 4;
        log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h, log2m, 3, new Edge(0, 2), new Edge(1, 3));

        final int newNode1 = 4;
        final int newNode2 = 5;

        assertEquals(1.0f, danf.count(2, h), epsilon);

        danf.addEdges(new Edge(2, newNode1), new Edge(3, newNode2));


        assertEquals(1.0f, danf.count(newNode1, h), epsilon);
        assertEquals(1.0f, danf.count(newNode2, h), epsilon);
        assertArrayEquals(new double[]{2.0,2.0,2.0,2.0}, danf.count(2), epsilon);
        assertArrayEquals(new double[]{2.0,2.0,2.0,2.0}, danf.count(3), epsilon);
        assertArrayEquals(new double[]{2.0,3.0,3.0,3.0}, danf.count(0), epsilon);
        assertArrayEquals(new double[]{2.0,3.0,3.0,3.0}, danf.count(1), epsilon);


    }

    /**
     * <pre>{@code
     * 0 -> 1 -> 2 -> 3N -> 4N
     * }</pre>
     */
    @Test
    public void checkPropagationToNonVCNode() throws IOException, InterruptedException {
        h = 3;
        log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h,log2m,2,new Edge(1,2));
        danf.addEdges(new Edge(0,1));

        final int newNode = 3;

        danf.addEdges(new Edge(3,4));
        danf.addEdges(new Edge(2,3));

        assertArrayEquals(new double[]{2.0,3.0,3.0},danf.count(2),epsilon);
        assertArrayEquals(new double[]{2.0,3.0,4.0},danf.count(1),epsilon);
        assertEquals(4.0,danf.count(0,h),epsilon);

    }

    /**
     * <pre>{@code
     * 0 -> N2
     *      |
     *      v
     * 1 -> N3
     * }</pre>
     */
    @Test
    public void testIncomingAndOutgoingEdgesMultipleNewNodes() throws IOException, InterruptedException {
        h = 4;
        log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h, log2m, 1);

        final int newNode1 = 2;
        final int newNode2 = 3;

        danf.addEdges(new Edge(0, newNode1), new Edge(1, newNode2), new Edge(newNode1, newNode2));

        assertArrayEquals(new double[]{2.0,3.0,3.0,3.0}, danf.count(0), epsilon);
        assertArrayEquals(new double[]{2.0,2.0,2.0,2.0}, danf.count(1), epsilon);
        assertArrayEquals(new double[]{2.0,2.0,2.0,2.0}, danf.count(newNode1), epsilon);
        assertArrayEquals(new double[]{1.0,1.0,1.0,1.0}, danf.count(newNode2), epsilon);
    }

    /**
     * <pre>{@code
     * N1 -> N2 -> 0
     * }</pre>
     */
    @Test
    public void testTwoNewNodes() throws IOException, InterruptedException {
        log2m = 10;
        h = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);

        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        Pair<DANF, HyperBoll> pair = TestUtils.runHyperBall(graph, dvc, h, log2m, fixedSeed);

        DANF danf = pair.getKey();

        danf.addEdges(new Edge(2, 0), new Edge(1, 2));

        assertArrayEquals(new double[]{2.0,2.0,2.0}, danf.count(2), epsilon);
        assertEquals(3.0, danf.count(1,h), epsilon);
    }

    /**
     * <pre>{@code
     * N2 -> 0
     * |
     * v
     * N3 -> 1
     * }</pre>
     */
    @Test
    public void testTwoNewNodesWithNeighbors() throws IOException, InterruptedException {
        log2m = 10;
        h = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);
        graph.addNode(1);

        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        Pair<DANF, HyperBoll> pair = TestUtils.runHyperBall(graph, dvc, h, log2m, fixedSeed);

        DANF danf = pair.getKey();

        danf.addEdges(new Edge(2, 3), new Edge(3, 1), new Edge(2, 0)  );

        assertArrayEquals(new double[]{3.0,4.0,4.0}, danf.count(2), epsilon);
        assertArrayEquals(new double[]{2.0,2.0,2.0}, danf.count(3), epsilon);
    }

    private DANF setupGraphAndRunHyperBall( int h, int log2m, long maxStartNode, Edge ... edges) throws IOException {
        SimulatedGraph graph = new SimulatedGraph();

        graph.addNode(maxStartNode);
        graph.addEdges(edges);

        DynamicVertexCover dvc = new DynamicVertexCover(graph);
        Pair<DANF, HyperBoll> pair = TestUtils.runHyperBall(graph, dvc, h, log2m, fixedSeed);
        DANF danf = pair.getKey();

        return danf;

    }
}
