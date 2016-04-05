package se.meltwater.test.vertexCover;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import org.junit.Test;
import static org.junit.Assert.*;

import se.meltwater.examples.VertexCover;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.LongStream;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Tests for the Dynamic Vertex Cover
 */
public class TestDynamicVertexCover {

    final int nrTestIterations = 100;

    /**
     * Tests that inserting nodes return the expected
     * affected nodes.
     */
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

    /**
     * Tests that when we delete edges from the VC we get the correct affected nodes back
     */
    @Test
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
    public void testVCSizeOnlyIncreaseInsertion() {
        int iteration = 0;
        final int maxNumNodes = 100;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = new SimulatedGraph();
            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            SimulatedGraph graphToMerge = TestUtils.genRandomGraph(maxNumNodes);
            graph.addNode(graphToMerge.getNumberOfNodes());

            Edge[] edges = graphToMerge.getAllEdges();

            long previousVCSize = 0;
            for (int i = 0; i < edges.length; i++) {
                graph.addEdge(edges[i]);
                dvc.insertEdge(edges[i]);

                long currentVCSize = dvc.getVertexCoverSize();

                assertTrue(previousVCSize <= currentVCSize);
                previousVCSize = currentVCSize;
            }
        }
    }

    @Test
    public void testVCSizeDeletions() {
        int iteration = 0;
        final int maxNumNodes = 100;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = new SimulatedGraph();
            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            SimulatedGraph graphToMerge = TestUtils.genRandomGraph(maxNumNodes);
            graph.addNode(graphToMerge.getNumberOfNodes());

            Edge[] edges = graphToMerge.getAllEdges();

            for (int i = 0; i < edges.length; i++) {
                dvc.insertEdge(edges[i]);
                graph.addEdge(edges[i]);
            }


            long previousVCSize = dvc.getVertexCoverSize();
            assertTrue(previousVCSize != 0);
            for (int i = 0; i < edges.length; i++) {
                graph.deleteEdge(edges[i]);
                dvc.deleteEdge(edges[i]);

                long currentVCSize = dvc.getVertexCoverSize();
                assertTrue(currentVCSize <= previousVCSize + 2); //+2 as removing some edges can increase the VC by at most two
                previousVCSize = currentVCSize;
            }
            assertTrue(previousVCSize == 0);
        }
    }

    /**
     * Tests that after sequential insertions and then deletions the total number
     * of nodes affected are zero.
     */
    @Test
    public void testSequentialInsertionsAndDeletions() throws CloneNotSupportedException {
        int iteration = 0;
        final int maxNumNodes = 100;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = new SimulatedGraph();
            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            SimulatedGraph graphToMerge = TestUtils.genRandomGraph(maxNumNodes);

            graph.addNode(graphToMerge.getNumberOfNodes());

            Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap = new HashMap<>();
            insertEdgesIntoVCAndUpdateAffected(graph, graphToMerge.getAllEdges(), dvc, affectedStateMap);

            /* Make sure there are affected nodes */
            assertTrue(affectedStateMap.size() > 0);

            deleteEdgesFromVCAndUpdateAffected(graph, dvc, affectedStateMap);
            /* After all have been inserted and deleted we should not have any affected nodes left */
            assertEquals(0, affectedStateMap.size());
        }
    }

    /**
     * Inserts {@code edges} into {@code to} and {@code dvc}.
     * Updates {@code affectedStateMap} with the resulting affected nodes.
     * @param to Graph to insert edges into
     * @param edges Edges to insert
     * @param dvc DVC to insert edges into
     * @param affectedStateMap Map to update with affected nodes
     */
    private void insertEdgesIntoVCAndUpdateAffected(SimulatedGraph to, Edge[] edges, DynamicVertexCover dvc, Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap) {
        for (int i = 0; i < edges.length; i++) {
            Edge edge = edges[i];
            to.addEdge(edge);

            Map<Long, IDynamicVertexCover.AffectedState> currentAffectedStateMap = dvc.insertEdge(edge);

            for (Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : currentAffectedStateMap.entrySet()) {
                DynamicVertexCover.updateAffectedNodes(entry.getKey(), entry.getValue(), affectedStateMap);
            }
        }
    }

    /**
     * Removes all edges from {@code graph} and {@code dvc} and updates
     * {@code affectedStateMap} with the resulting affected nodes.
     * @param graph Graph to delete from
     * @param dvc DVC to delete from
     * @param affectedStateMap Map to update
     */
    private void deleteEdgesFromVCAndUpdateAffected(SimulatedGraph graph, DynamicVertexCover dvc, Map<Long, IDynamicVertexCover.AffectedState> affectedStateMap) {
        Edge[] edges = graph.getAllEdges();

        for (Edge edge : edges) {
            boolean existed = graph.deleteEdge(edge);
            if(existed) {
                Map<Long, IDynamicVertexCover.AffectedState> currentAffectedStateMap = dvc.deleteEdge(edge);
                for (Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : currentAffectedStateMap.entrySet()) {
                    DynamicVertexCover.updateAffectedNodes(entry.getKey(), entry.getValue(), affectedStateMap);
                }
            }
        }
    }




    /**
     * Small test that inserts a small graph into the dynamic
     * vertex cover and assures that the resulting vertex cover
     * actually is a vertex cover.
     */
    @Test
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
    public void testEmptyGraph() {
        SimulatedGraph graph = new SimulatedGraph();
        DynamicVertexCover dvc = new DynamicVertexCover(graph);
        assertTrue(isVertexCover(graph, dvc));
    }

    /**
     * Randomly generates a graph and sequentially inserts
     * edges into the dynamic vertex cover. After each insertion
     * we assert that it is a VC. Then we delete the edges
     * one by one in a permutated order and assure that it still is a VC.
     */
    @Test
    public void testRandomInsertionsAndDeletions() {
        int iteration = 0;

        while(iteration++ < nrTestIterations) {
            final int maxNumNodes = 50;

            SimulatedGraph graph = TestUtils.genRandomGraph(maxNumNodes);
            Edge[] edges = graph.getAllEdges();

            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            for (int i = 0; i < edges.length; i++) {
                graph.addEdge(edges[i]);
                dvc.insertEdge(edges[i]);
                assertTrue(isVertexCover(graph, dvc));
            }

            /* Permute array so it doesnt have to be deleted in the same order as inserted */
            ArrayList<Edge> edgeList = new ArrayList<>(Arrays.asList(edges));
            Collections.shuffle(edgeList);

            /* Delete edges in permutated order */
            for (int i = 0; i < edges.length; i++) {
                Edge edge = edgeList.get(i);
                graph.deleteEdge(edge);
                dvc.deleteEdge(edge);
                boolean isgs = isVertexCover(graph, dvc);
                if(!isgs) {
                    System.out.println("h");
                }
                assertTrue(isVertexCover(graph, dvc));
            }
        }
    }

    /**
     * Tests that after we delete an edge in the Maximal Matching
     * we still have a VC.
     */
    @Test
    public void testDeletionsInMaximal() {
        long[] nodes = {0, 1, 2, 3};
        Edge[] edges = {new Edge(nodes[0], nodes[2]),
                new Edge(nodes[1], nodes[2])};

        SimulatedGraph graph = TestUtils.setupSGraph(nodes, edges);
        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        Edge edgeInMaximalMatching = edges[0];
        graph.deleteEdge(edgeInMaximalMatching);
        dvc.deleteEdge(edgeInMaximalMatching);

        assertTrue(isVertexCover(graph, dvc));
    }

    /**
     * Tests that after we delete an edge that is NOT in the
     * maximal matching we still have a VC (should be unaffected)
     */
    @Test
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

    /**
     * Inserts a generated graph into the VC and then deletes all edges.
     * At the end, we should have a VC of size 0 and a Maximal matching
     * of size 0
     */
    @Test
    public void testDeleteAllEdges() {
        final int maxNumNodes = 6;
        int iteration = 99;

        while(iteration++ < nrTestIterations) {
            SimulatedGraph graph = TestUtils.genRandomGraph(maxNumNodes);
            Edge[] edges = graph.getAllEdges();

            DynamicVertexCover dvc = new DynamicVertexCover(graph);

            for (Edge edge : edges) {
                graph.deleteEdge(edge);
                dvc.deleteEdge(edge);
                assertTrue(isVertexCover(graph, dvc));
            }

            assertEquals(0, dvc.getVertexCoverSize());
            assertEquals(0, dvc.getMaximalMatchingSize());
        }
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


    /**
     * Tests that the iterator in Vertex Cover returns the
     * correct node
     */
    @Test
    public void testVertexCoverIterator(){
        int nodes = new Random().nextInt(1000);
        DynamicVertexCover vc = new DynamicVertexCover(TestUtils.genRandomGraph(nodes));

        LazyLongIterator vcIterator = vc.getNodesInVertexCoverIterator();
        LongArrayBitVector vcNodes = vc.getNodesInVertexCover();
        long node = 0;
        while((node = vcNodes.nextOne(node)) != -1) {
            assertEquals(node,vcIterator.nextLong());

            node = node + 1;
        }

        /*long[] nodesInVC = vc.getNodesInVertexCover();
        LazyLongIterator vcIterator = vc.getNodesInVertexCoverIterator();
        for (long node: nodesInVC ) {
            assertEquals(node,vcIterator.nextLong());
        }*/
    }
}
