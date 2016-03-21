package se.meltwater.test.vertexCover;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import org.junit.Test;
import static org.junit.Assert.*;

import se.meltwater.examples.VertexCover;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.LongStream;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Tests for the Dynamic Vertex Cover
 */
public class TestDynamicVertexCover {

    final int nrTestIterations = 100;

    @Test
    public void testInsertAffectedNodes() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);
        Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap = dvc.insertEdge(edges[0]);

        /* Both endpoints should've been added and none more */
        assertTrue(affectedStateMap.get(edges[0].from) == IDynamicVertexCover.AffectedState.Added);
        assertTrue(affectedStateMap.get(edges[0].to)   == IDynamicVertexCover.AffectedState.Added);
        assertTrue(affectedStateMap.size() == 2);

        /* The from node of this edge should already be covered => no affected nodes */
        affectedStateMap = dvc.insertEdge(edges[1]);
        assertTrue(affectedStateMap.size() == 0);

        /* The to node of this edge should already be covered => no affected nodes */
        affectedStateMap = dvc.insertEdge(edges[2]);
        assertTrue(affectedStateMap.size() == 0);
    }

    @Test
    /**
     * Tests that when we delete edges from the VC we get the correct affected nodes back
     */
    public void testDeleteAffectedNodes() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        Edge removedEdge = edges[0];
        graph.deleteEdge(removedEdge);
        Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap = dvc.deleteEdge(removedEdge);

        /* First node dont have any more edges so should be removed from VC */
        assertTrue(affectedStateMap.get(nodes[0]) == IDynamicVertexCover.AffectedState.Removed);

        /* Third node are no longer covered by second node so it should be added together with the second */
        assertTrue(affectedStateMap.get(nodes[2]) == IDynamicVertexCover.AffectedState.Added);

        /* Second node were already in VC so shouldnt be affected */
        assertTrue(affectedStateMap.get(nodes[1]) == null);
        assertTrue(affectedStateMap.size() == 2);
    }

    @Test
    /**
     * Tests that after sequential insertions and then deletions the total number
     * of nodes affected are zero.
     */
    public void testSequentialInsertionsAndDeletions() throws CloneNotSupportedException {
        int iteration = 0;
        final int maxNumNodes = 100;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = new SimulatedGraph();
            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            SimulatedGraph graphToMerge = TestUtils.genRandomGraph(maxNumNodes);

            for(int i = 0; i < graphToMerge.getNumberOfNodes(); i++){
                graph.addNode(i);
            }

            Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap = new HashMap<>();
            insertEdgesIntoVCAndUpdateAffected(graphToMerge, graph, dvc, affectedStateMap);

            /* Make sure there are affected nodes */
            assertTrue(affectedStateMap.size() > 0);

            deleteEdgesFromVCAndUpdateAffected(graph, dvc, affectedStateMap);
            /* After all have been inserted and deleted we should not have any affected nodes left */
            assertEquals(0, affectedStateMap.size());
        }
    }

    private void insertEdgesIntoVCAndUpdateAffected(SimulatedGraph from, SimulatedGraph to, DynamicVertexCover dvc, Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap) {
        from.iterateAllEdges(edge -> {
            to.addEdge(edge);

            Map<Long, IDynamicVertexCover.AffectedState> currentAffectedStateMap = dvc.insertEdge(edge);

            for (Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : currentAffectedStateMap.entrySet()) {
                DynamicVertexCover.updateAffectedNodes(entry.getKey(), entry.getValue(), affectedStateMap);
            }

            return null;
        });
    }

    private void deleteEdgesFromVCAndUpdateAffected(SimulatedGraph graph, DynamicVertexCover dvc, Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap) {

        long numNodes = graph.getNumberOfNodes();
        for (int i = 0; i < numNodes ; i++) {
            for (int j = 0; j < numNodes ; j++) {
                Edge edge = new Edge(i,j);
                boolean existed = graph.deleteEdge(edge);
                if(existed) {
                    Map<Long, IDynamicVertexCover.AffectedState> currentAffectedStateMap = dvc.deleteEdge(edge);
                    for (Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : currentAffectedStateMap.entrySet()) {
                        DynamicVertexCover.updateAffectedNodes(entry.getKey(), entry.getValue(), affectedStateMap);
                    }
                }
            }
        }
    }

    @Test
    /**
     * Small test that inserts a small graph into the dynamic
     * vertex cover and assures that the resulting vertex cover
     * actually is a vertex cover.
     */
    public void testInsertions() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        assertTrue(isVertexCover(graph, dvc));

    }

    @Test
    /**
     * Randomly generates a graph and sequentially inserts
     * edges into the dynamic vertex cover. After each insertion
     * we assert that it is a VC. Then we delete the edges
     * one by one and assure that it still is a VC.
     */
    public void testRandomInsertionsAndDeletions() {
        final int maxNumNodes = 100;
        Random rand = new Random();
        int n = rand.nextInt(maxNumNodes);
        int m = rand.nextInt((int)Math.pow(n, 2));

        long[] nodes = LongStream.rangeClosed(0, n).toArray();
        Edge[] edges = TestUtils.generateEdges(n, m);

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, new Edge[0]);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        /*TODO Possible improvement: Calculate a random permutation of edges and insert them in that order
        and the same for deletion */

        for(int i = 0; i < edges.length; i++) {
            graph.addEdge(edges[i]);
            dvc.insertEdge(edges[i]);
            assertTrue(isVertexCover(graph,dvc));
        }

        for(int i = 0; i < edges.length; i++) {
            graph.deleteEdge(edges[i]);
            dvc.deleteEdge(edges[i]);
            assertTrue(isVertexCover(graph,dvc));
        }
    }

    @Test
    /**
     * Tests that after we delete an edge in the Maximal Matching
     * we still have a VC.
     */
    public void testDeletionsInMaximal() {
        long[] nodes = {0, 1, 2, 3};
        Edge[] edges = {new Edge(nodes[0], nodes[2]),
                new Edge(nodes[1], nodes[2])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        graph.deleteEdge(edges[0]);
        dvc.deleteEdge(edges[0]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    /**
     * Tests that after we delete an edge that is NOT in the
     * maximal matching we still have a VC (should be unaffected)
     */
    public void testDeletionsOutsideMaximal() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        graph.deleteEdge(edges[1]);
        dvc.deleteEdge(edges[1]);

        assertTrue(isVertexCover(graph, dvc));
    }

    @Test
    /**
     * Inserts a small graph into the VC and then deleted them all.
     * At the end, we should have a VC of size 0 and a Maximal matching
     * of size 0
     */
    public void testDeleteAllEdges() {
        long[] nodes = {0, 1, 2};
        Edge[] edges = {new Edge(nodes[0], nodes[1]),
                new Edge(nodes[1], nodes[2]),
                new Edge(nodes[2], nodes[0])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        for(Edge edge : edges ) {
            graph.deleteEdge(edge);
            dvc.deleteEdge(edge);
        }

        assertTrue(isVertexCover(graph, dvc));
        assertTrue(dvc.getVertexCoverSize() == 0);
        assertTrue(dvc.getMaximalMatchingSize() == 0);
    }



    /**
     * Assures that all edges in {@code graph} is covered in {@code dvc}
     * @param graph
     * @param dvc
     * @return
     */
    public boolean isVertexCover(SimulatedGraph graph, DynamicVertexCover dvc) {
        Boolean failedValue = graph.iterateAllEdges(edge -> {
            if(!(dvc.isInVertexCover(edge.from) || dvc.isInVertexCover(edge.to))) {
                return new Boolean(false);
            }
            return null;
        });

        if(failedValue == null) {
            return true;
        }

        return false;
    }

    @Test
    public void testVertexCoverIterator(){
        int nodes = new Random().nextInt(1000);
        DynamicVertexCover vc = new DynamicVertexCover(TestUtils.genRandomGraph(nodes));
        long[] nodesInVC = vc.getNodesInVertexCover();
        LazyLongIterator vcIterator = vc.getNodesInVertexCoverIterator();
        for (long node: nodesInVC ) {
            assertEquals(node,vcIterator.nextLong());
        }
    }
}
