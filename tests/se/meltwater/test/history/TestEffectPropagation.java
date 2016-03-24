package se.meltwater.test.history;

import javafx.util.Pair;
import org.junit.Test;
import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;

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

    final float epsilon = 0.05f;

    int log2m;
    int h;

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

        assertEquals(3.0f, danf.count(0, h), epsilon);
        assertEquals(3.0f, danf.count(1, h), epsilon);
        assertEquals(2.0f, danf.count(2, h), epsilon);
        assertEquals(1.0f, danf.count(3, h), epsilon);
    }

    /**
     *
     * The graph:
     * <pre>{@code
     *  0N
     *  |
     *  v
     *  2-->3
     *  ^
     *  |
     *  1N
     *  }</pre>
     */
    @Test
    public void testTwoNewNodesCorrectRecalculation() throws InterruptedException, IOException {
        log2m = 10;
        h = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(3); /* Must be a node in the graph for HBoll */
        graph.addEdge(new Edge(2,3));

        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        Pair<DANF, HyperBoll> pair = TestUtils.runHyperBall(graph, dvc, h, log2m, fixedSeed);

        DANF danf = pair.getKey();

        danf.addEdges(new Edge(1, 2));
        //danf.addEdge(new Edge(0,2));

        //assertEquals(3.0, danf.count(0, h), epsilon);
        assertEquals(3.0, danf.count(1, h), epsilon);
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

        assertEquals(4.0f, danf.count(0, h), epsilon);
        assertEquals(4.0f, danf.count(1, h), epsilon);
        assertEquals(1.0f, danf.count(2, h), epsilon);
        assertEquals(1.0f, danf.count(3, h), epsilon);
        assertEquals(3.0f, danf.count(newNode, h), epsilon);
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

        danf.addEdges(new Edge(2, newNode1), new Edge(3, newNode2));

        assertEquals(3.0f, danf.count(0, h), epsilon);
        assertEquals(3.0f, danf.count(1, h), epsilon);
        assertEquals(2.0f, danf.count(2, h), epsilon);
        assertEquals(2.0f, danf.count(3, h), epsilon);
        assertEquals(1.0f, danf.count(newNode1, h), epsilon);
        assertEquals(1.0f, danf.count(newNode2, h), epsilon);
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

        assertEquals(3.0f, danf.count(0, h), epsilon);
        assertEquals(2.0f, danf.count(1, h), epsilon);
        assertEquals(2.0f, danf.count(newNode1, h), epsilon);
        assertEquals(1.0f, danf.count(newNode2, h), epsilon);
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

        assertEquals(2.0, danf.count(2, h), epsilon);
        assertEquals(3.0, danf.count(1, h), epsilon);
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

        assertEquals(4.0, danf.count(2, h), epsilon);
        assertEquals(2.0, danf.count(3, h), epsilon);
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
