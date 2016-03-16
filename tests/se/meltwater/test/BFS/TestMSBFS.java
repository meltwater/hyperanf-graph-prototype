package se.meltwater.test.BFS;

import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.big.webgraph.BVGraph;
import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.MSBreadthFirst;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;

/**
 * Class resposible for testing that MSBFS traverses
 * a graph correctly.
 */
public class TestMSBFS {

    private final int maxIterations = 50;
    private final int maxGraphSize  = 100;

    @Test
    /**
     * Loads a random simulated graph and tests MSBFS on it.
     */
    public void testOnGeneratedGraph() throws InterruptedException {
        int iteration = 0;
        while(iteration++ < maxIterations ) {
            int numNodes = new Random().nextInt(maxGraphSize - 1) + 1; /* Make sure numNodes always positive */
            SimulatedGraph graph = TestUtils.genRandomGraph(numNodes);
            testGraph(graph);
        }
    }

    @Test
    /**
     * Loads a real graph file without blocks and tests MSBFS on it.
     */
    public void testBFSValidity() throws IOException, InterruptedException {
        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        testGraph(graph);
    }

    /**
     * Performs a MSBFS with random sources on {@code graph} and makes sure that the sources gets the same result
     * as a standard BFS algorithm would.
     * @param graph
     * @throws InterruptedException
     */
    public void testGraph(IGraph graph) throws InterruptedException {
        int[] bfsSources = generateSources((int) graph.getNumberOfNodes());
        MSBreadthFirst msbfs = new MSBreadthFirst(bfsSources, graph);
        BitSet[] seen = msbfs.breadthFirstSearch();
        checkValidSeen(bfsSources, seen, graph);
    }

    @Test
    /**
     * Performs a MSBFS with a visitor on a real graph. The visitor should only visit
     * its source node and then stop the propagation.
     */
    public void testBFSVisitorOnlySources() throws IOException, InterruptedException {
        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        int[] bfsSources = generateSources((int) graph.getNumberOfNodes());
        ArrayList<AssertionError> errors = new ArrayList<>();
        MSBreadthFirst msbfs = new MSBreadthFirst(bfsSources, graph, onlySourcesVisitor(bfsSources, errors));
        msbfs.breadthFirstSearch();
        assertEquals(0,errors.size());
    }

    /**
     * Returns a visitor that should only visit its source node.
     * The visitor asserts that it never visits another node.
     * @param bfsSources
     * @return
     */
    public MSBreadthFirst.Visitor onlySourcesVisitor(int[] bfsSources, ArrayList<AssertionError> errors){
        return (long node, BitSet visitedBy, BitSet seen, int iteration) -> {
            try {
            /* An iteration > 0 implies a bfs have reached a node != its source */
                assertEquals("Iteration for node " + node + " should be zero", 0, iteration);

                int bfs = -1;
            /* Assert that the only bfs that have visited this node is the bfs that had it as source */
                while ((bfs = visitedBy.nextSetBit(bfs + 1)) != -1)
                    assertEquals("Node " + node + " should be visited by " + bfs, bfsSources[bfs], node);

            /* Stop this bfs propagation */
                visitedBy.clear();
            } catch (AssertionError error) {
                errors.add(error);
                visitedBy.clear();
            }
        };
    }

    @Test
    /**
     * Tests a MSBFS with a visitor that should visit itself and all its neighbors.
     */
    public void testBFSVisitorNeighborsCorrect() throws IOException, InterruptedException {
        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        int[] bfsSources = generateSources((int) graph.getNumberOfNodes());
        ArrayList<AssertionError> errors = new ArrayList<>();

        MSBreadthFirst msbfs = new MSBreadthFirst(bfsSources, graph,neighborsCorrect(bfsSources,graph,errors));
        msbfs.breadthFirstSearch();

        assertEquals(errors.size() + " visitors reported assertion errors" ,0,errors.size());
    }

    /**
     * Returns a visitor that should visit itself and its immediate neighbors.
     * Asserts that it never travels farther than 1 step.
     * @param bfsSources
     * @param graph
     * @param errors
     * @return
     */
    public MSBreadthFirst.Visitor neighborsCorrect(int[] bfsSources, IGraph graph, ArrayList<AssertionError> errors){
        BitSet[] shouldHave = new BitSet[(int)graph.getNumberOfNodes()];
        BitSet[] alreadySeen = new BitSet[(int)graph.getNumberOfNodes()];

        for(int node = 0; node < graph.getNumberOfNodes() ; node++) {
            shouldHave[node] = new BitSet(bfsSources.length);
            alreadySeen[node] = new BitSet(bfsSources.length);
        }

        return (long node, BitSet visitedBy, BitSet seen, int iteration) -> {
            synchronized (this) {
                try {
                    assertFalse("Iteration should not be more than one", iteration > 1);
                    if (iteration == 1) {
                        shouldHave[(int) node].andNot(alreadySeen[(int) node]);
                        BitSet clonedShouldHave = (BitSet) shouldHave[(int) node].clone();
                        clonedShouldHave.and(visitedBy);
                        assertEquals("Node " + node, clonedShouldHave.cardinality(), shouldHave[(int) node].cardinality());

                        visitedBy.clear();
                    } else {
                        graph.setNodeIterator(node);
                        long out = (int) graph.getOutdegree(), neigh;
                        alreadySeen[(int) node].or(visitedBy);

                        for (int neighI = 0; neighI < out; neighI++) {
                            neigh = graph.getNextNeighbor();
                            shouldHave[(int) neigh].or(visitedBy);
                        }
                    }
                } catch (AssertionError e) {
                    errors.add(e);
                    visitedBy.clear();
                }
            }
        };
    }

    /**
     * Randomly selects 100 sources from {@code numNodes}
     * @param numNodes
     * @return
     */
    private int[] generateSources(int numNodes){
        Random rand = new Random(System.currentTimeMillis());
        int numSources = 100;
        int[] sources = new int[numSources];
        for(int i = 0; i<numSources; i++)
            sources[i] = rand.nextInt(numNodes-1);
        return sources;

    }

    /**
     * Performs a standard BFS from each source and makes sure that the MSBFS detected the same nodes
     * @param sources
     * @param seen
     * @param graph
     */
    private void checkValidSeen(int[] sources, BitSet[] seen, IGraph graph){
        BitSet[] originalSeen = new BitSet[seen.length];
        int i = 0;
        for (BitSet bits : seen)
            originalSeen[i++] = (BitSet)bits.clone();

        /* Do standard BFS from each source */
        for (int bfs = 0; bfs < sources.length ; bfs++) {
            IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
            queue.enqueue(sources[bfs]);
            BitSet nodesChecked = new BitSet((int)graph.getNumberOfNodes());
            nodesChecked.set(sources[bfs]);

            assertTrue("Source node " + sources[bfs] + " for bfs " + bfs,seen[sources[bfs]].get(bfs));

            seen[sources[bfs]].clear(bfs);

            /* Do bfs from current node */
            while (!queue.isEmpty()) {
                int curr = queue.dequeueInt();
                graph.setNodeIterator(curr);
                int d = (int)graph.getOutdegree();

                /* Visit all neighbors */
                while (d-- != 0) {
                    int succ = (int)graph.getNextNeighbor();

                    if (!nodesChecked.get(succ)) {
                        nodesChecked.set(succ);
                        queue.enqueue(succ);

                        /* Make sure the MSBFS also have seen the node */
                        assertTrue("Node " + succ + " for bfs " + bfs,seen[succ].get(bfs));

                        /* Clearing the bit is used to make sure the MSBFS have not seen to many  */
                        seen[succ].clear(bfs);
                    }
                }
            }
        }

        /* If any MSBFS have any bit still set it means it have seen more than the standard BFS did */
        for(int node = 0; node<seen.length; node++){
            assertEquals("Node " + node + " should be clear",0,seen[node].cardinality());
        }
    }
}
