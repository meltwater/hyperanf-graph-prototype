package se.meltwater.test.topNodeCounter;

import javafx.util.Pair;
import org.junit.Test;
import se.meltwater.algo.DANF;
import se.meltwater.algo.TopNodeCounter;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.util.HashSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * // TODO Class description
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class TestTopNodeCounter {

    private final double epsilon = 0.05;
    private final int maxIterations = 100;

    final int h = 5;
    final int log2m = 7;
    final int updateIntervalms = 0;
    final double percentageChange = 1.0;
    final int minNodeCount = 0;
    final long seed = 0L;

    /**
     * Tests that the callback is called when an edge is added
     * and the updateInterval time has passed
     * @throws InterruptedException
     */
    @Test
    public void testCallbackIsCalled() throws InterruptedException {
        IGraph graph = new SimulatedGraph();
        graph.addEdges(new Edge(0,1));
        DANF danf = new DANF(h, log2m, graph);
        TopNodeCounter topNodeCounter = new TopNodeCounter(danf, updateIntervalms, percentageChange, minNodeCount );

        AtomicBoolean wasCalled = new AtomicBoolean(false);
        topNodeCounter.setRapidChangeCallback(set -> {
            wasCalled.set(true);
        });

        Edge[] edgesToAdd = {new Edge(0,1)};
        danf.addEdges(edgesToAdd);
        topNodeCounter.updateNodeSets(edgesToAdd);

        assertTrue(wasCalled.get());
    }

    /**
     * Tests that the callback is called with a correct set of node/values
     * @throws InterruptedException
     */
    @Test
    public void testCallbackIsCalledWithCorrectNodes() throws InterruptedException {
        final long nodeThatShouldChange = 0;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);

        DANF danf = new DANF(h, log2m, graph, seed);
        TopNodeCounter topNodeCounter = new TopNodeCounter(danf, updateIntervalms, percentageChange, minNodeCount );

        topNodeCounter.setRapidChangeCallback(set -> {
            assertEquals(1, set.size()); // Only node 0 should have had its value changed
            for (Pair<Double, Long> pair : set) {
                assertEquals(nodeThatShouldChange, pair.getValue().longValue());

                final double shouldHaveValue = 2;
                final double shouldHaveHadValue = 1;
                final double expectedIncrease = shouldHaveValue / shouldHaveHadValue;

                assertEquals(pair.getKey(), expectedIncrease, epsilon);
            }
        });

        Edge[] edgesToAdd = {new Edge(nodeThatShouldChange,1)};
        danf.addEdges(edgesToAdd);
        topNodeCounter.updateNodeSets(edgesToAdd);
    }

    @Test
    /**
     * Tests that all nodes that should be in the sorted node/value list are in it.
     */
    public void testAllNodesAreInSortedList() throws InterruptedException {
        final int nMax = 100;

        int iteration = 0;
        while(iteration++ < maxIterations) {
            SimulatedGraph graph = new SimulatedGraph();
            graph.addNode(0);

            DANF danf = new DANF(h, log2m, graph, seed);
            TopNodeCounter topNodeCounter = new TopNodeCounter(danf, updateIntervalms, percentageChange, minNodeCount);

            SimulatedGraph graphToInsert = TestUtils.genRandomGraph(nMax);
            Edge[] edgesToAdd = graphToInsert.getAllEdges();
            danf.addEdges(edgesToAdd);
            topNodeCounter.updateNodeSets(edgesToAdd);

            HashSet<Long> nodesInEdgesToAdd = getNodesInEdges(edgesToAdd);
            HashSet<Long> nodesInSortedSet = getNodesFromSetPair(topNodeCounter);

            assertEquals(nodesInSortedSet.size(), nodesInEdgesToAdd.size());
        }
    }

    /**
     * Returns all unique nodes that are present in sorted node/value list of {@code topNodeCounter}
     * @param topNodeCounter
     * @return
     */
    private HashSet<Long> getNodesFromSetPair(TopNodeCounter topNodeCounter) {
        TreeSet<Pair<Double, Long>> sortedSet = topNodeCounter.getNodesSortedByValue();
        HashSet<Long> nodesInSortedSet = new HashSet<>();
        sortedSet.forEach(pair -> {
            nodesInSortedSet.add(pair.getValue());
        });
        return nodesInSortedSet;
    }

    /**
     * Returns all unique nodes that are present in {@code edgesToAdd}
     * @param edgesToAdd
     * @return
     */
    private HashSet<Long> getNodesInEdges(Edge[] edgesToAdd) {
        HashSet<Long> nodesInEdgesToAdd = new HashSet<>();
        nodesInEdgesToAdd.add(0L); //Required as all nodes present in initiation of topcounter will be in the sorted list
        for (int i = 0; i < edgesToAdd.length; i++) {
            Edge edge = edgesToAdd[i];
            nodesInEdgesToAdd.add(edge.from);
            nodesInEdgesToAdd.add(edge.to);
        }
        return nodesInEdgesToAdd;
    }

    @Test
    public void testSortedListIsUnchangedAfterSameEdgeInsertions() throws InterruptedException {
        final int nMax = 100;

        int iteration = 0;
        while(iteration++ < maxIterations) {
            SimulatedGraph graph = new SimulatedGraph();
            graph.addNode(0);

            DANF danf = new DANF(h, log2m, graph, seed);
            TopNodeCounter topNodeCounter = new TopNodeCounter(danf, updateIntervalms, percentageChange, minNodeCount);

            SimulatedGraph graphToInsert = TestUtils.genRandomGraph(nMax);
            Edge[] edgesToAdd = graphToInsert.getAllEdges();
            danf.addEdges(edgesToAdd);
            topNodeCounter.updateNodeSets(edgesToAdd);

            TreeSet<Pair<Double, Long>> valuesAfterFirstInsertion =  topNodeCounter.getNodesSortedByValue();
            Pair[] valuesAfterFirstInsertionArray = new Pair[valuesAfterFirstInsertion.size()];
            valuesAfterFirstInsertionArray = valuesAfterFirstInsertion.toArray(valuesAfterFirstInsertionArray);

            danf.addEdges(edgesToAdd);
            topNodeCounter.updateNodeSets(edgesToAdd);
            TreeSet<Pair<Double, Long>> valuesAfterSecondInsertion =  topNodeCounter.getNodesSortedByValue();
            Pair[] valuesAfterSecondInsertionArray = new Pair[valuesAfterSecondInsertion.size()];
            valuesAfterSecondInsertionArray = valuesAfterFirstInsertion.toArray(valuesAfterSecondInsertionArray);

            assertArrayEquals(valuesAfterFirstInsertionArray, valuesAfterSecondInsertionArray);
        }
    }
}
