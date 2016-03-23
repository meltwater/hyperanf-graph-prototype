package se.meltwater.test.history;

import it.unimi.dsi.big.webgraph.NodeIterator;
import javafx.util.Pair;
import org.junit.Test;
import se.meltwater.DANF;
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

    @Test
    /**
     * 0 -> 2 -> N3
     * 1 ---∧
     */
    public void testOnlyIncomingEdgesSingleNewNode() throws InterruptedException, IOException {
        final int h = 4;
        final int log2m = 7;

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

    @Test
    /**
     * 0 -> N4 -> 2
     * 1 --∧ v--> 3
     */
    public void testIncomingAndOutgoingEdgesSingleNewNode() throws IOException, InterruptedException {
        final int h = 4;
        final int log2m = 7;

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

    @Test
    /**
     * 0 -> 2 -> N4
     * 1 -> 3 -> N5
     */
    public void testOnlyIncomingEdgesMultipleNewNodes() throws IOException, InterruptedException {
        final int h = 4;
        final int log2m = 7;

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

    @Test
    /**
     * 0 -> N2
     *      |
     *      v
     * 1 -> N3
     */
    public void testIncomingAndOutgoingEdgesMultipleNewNodes() throws IOException, InterruptedException {
        final int h = 4;
        final int log2m = 7;

        DANF danf = setupGraphAndRunHyperBall(h, log2m, 1);

        final int newNode1 = 2;
        final int newNode2 = 3;

        danf.addEdges(new Edge(0, newNode1), new Edge(1, newNode2), new Edge(newNode1, newNode2));

        assertEquals(3.0f, danf.count(0, h), epsilon);
        assertEquals(2.0f, danf.count(1, h), epsilon);
        assertEquals(2.0f, danf.count(newNode1, h), epsilon);
        assertEquals(1.0f, danf.count(newNode2, h), epsilon);
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
