package se.meltwater.test.BFS;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import se.meltwater.bfs.MSBreadthFirst;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.test.TestUtils;
import se.meltwater.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class resposible for testing that MSBFS traverses
 * a graph correctly.
 */
public class TestMSBFS {

    private final int maxIterations = 50;
    private final int maxGraphSize  = 100;

    /**
     * Tests that the traveler to a visitor is never {@code null}
     */
    @Test
    public void testTravelerNeverNull() throws InterruptedException {
        int iteration = 0;
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        while (iteration++ < maxIterations){
            long numNodes = rand.nextLong(maxGraphSize - 1) + 1; /* Make sure numNodes always positive */
            SimulatedGraph graph = TestUtils.genRandomGraph((int)numNodes);
            numNodes = graph.getNumberOfNodes();
            long sources[] = TestUtils.generateRandomNodes(numNodes,(int)numNodes,1);
            MSBreadthFirst.Traveler t = (MSBreadthFirst.Traveler t1, int depth) ->  t1;
            MSBreadthFirst.Traveler[] travelers = Utils.repeat(t,sources.length, new MSBreadthFirst.Traveler[0]);
            AtomicBoolean noNull = new AtomicBoolean(true);
            MSBreadthFirst.Visitor v = (long x, BitSet y, BitSet z, int d, MSBreadthFirst.Traveler t2) -> {if(t2 == null) noNull.set(false);};
            MSBreadthFirst msbfs = new MSBreadthFirst(graph);
            msbfs.breadthFirstSearch(sources,v,travelers);
            msbfs.close();
            assertTrue(noNull.get());
        }
    }



    /**
     * Tests that a traveler merges once and only once for a graph where that should happen. Starts
     * at node 0 and 1.
     * The graph:
     * <pre>{@code
     *  0
     *  |
     *  v
     *  2-->3
     *  ^
     *  |
     *  1
     *  }</pre>
     */
    @Test
    public void testOneMerge() throws InterruptedException {

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(3);
        graph.addEdges(new Edge(0, 2), new Edge(1, 2), new Edge(2, 3));
        long[] bfsSources = new long[]{0, 1};
        AtomicInteger merges = new AtomicInteger(0);
        MSBreadthFirst.Traveler[] travs = new CountMergesTraveler[]{new CountMergesTraveler(merges), new CountMergesTraveler(merges)};
        MSBreadthFirst bfs = new MSBreadthFirst(graph);
        bfs.breadthFirstSearch(bfsSources, correctMergesVisitor(), travs);
        bfs.close();
        assertEquals(1, merges.get());

    }

    /**
     * used by {@link TestMSBFS#testOneMerge()} To check that the merge is done at the correct location
     * @return
     */
    private MSBreadthFirst.Visitor correctMergesVisitor(){
        return (long visitNode, BitSet bfsVisits,BitSet seen, int depth, MSBreadthFirst.Traveler trav) -> {
            CountMergesTraveler traveler = (CountMergesTraveler) trav;
            switch ((int)visitNode){
                case 0:
                    assertTrue(bfsVisits.get(0));
                    assertFalse(bfsVisits.get(1));
                    assertEquals(traveler.merges,0);
                    break;
                case 1:
                    assertTrue(bfsVisits.get(1));
                    assertFalse(bfsVisits.get(0));
                    assertEquals(traveler.merges,0);
                    break;
                case 2:
                    assertTrue(bfsVisits.get(1));
                    assertTrue(bfsVisits.get(0));
                    assertEquals(traveler.merges,1);
                    break;
                case 3:
                    assertTrue(bfsVisits.get(1));
                    assertTrue(bfsVisits.get(0));
                    assertEquals(traveler.merges,1);
            }
        };
    }

    /**
     * Used by {@link TestMSBFS#testOneMerge()} to keep track of the number of merges.
     */
    private class CountMergesTraveler implements MSBreadthFirst.Traveler{
        private int merges = 0;
        private AtomicInteger totMerges;

        public CountMergesTraveler(AtomicInteger totMergers){
            this.totMerges = totMergers;
        }

        @Override
        public MSBreadthFirst.Traveler merge(MSBreadthFirst.Traveler mergeWith, int depth) {
            CountMergesTraveler clone = new CountMergesTraveler(totMerges);
            clone.merges = ((CountMergesTraveler)mergeWith).merges + merges + 1;
            totMerges.incrementAndGet();
            assertEquals(1,depth);
            return clone;
        }
    }

    /**
     * Loads a random simulated graph and tests MSBFS on it.
     */
    @Test
    public void testOnGeneratedGraph() throws InterruptedException {
        int iteration = 0;
        while(iteration++ < maxIterations ) {
            int numNodes = new Random().nextInt(maxGraphSize - 1) + 1; /* Make sure numNodes always positive */
            SimulatedGraph graph = TestUtils.genRandomGraph(numNodes);
            testGraph(graph);
        }
    }

    /**
     * Loads a real graph file without blocks and tests MSBFS on it.
     */
    @Test
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
        long[] bfsSources = generateSources(graph.getNumberOfNodes());
        MSBreadthFirst msbfs = new MSBreadthFirst(graph);
        BitSet[][] seen = msbfs.breadthFirstSearch(bfsSources);
        msbfs.close();
        checkValidSeen(bfsSources, seen, graph);
    }

    /**
     * Performs a MSBFS with a visitor on a real graph. The visitor should only visit
     * its source node and then stop the propagation.
     */
    @Test
    public void testBFSVisitorOnlySources() throws IOException, InterruptedException {
        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        long[] bfsSources = generateSources(graph.getNumberOfNodes());
        ArrayList<AssertionError> errors = new ArrayList<>();
        MSBreadthFirst msbfs = new MSBreadthFirst(graph);
        msbfs.breadthFirstSearch(bfsSources,onlySourcesVisitor(bfsSources, errors));
        msbfs.close();
        assertEquals(0,errors.size());
    }

    /**
     * Returns a visitor that should only visit its source node.
     * The visitor asserts that it never visits another node.
     * @param bfsSources
     * @return
     */
    public MSBreadthFirst.Visitor onlySourcesVisitor(long[] bfsSources, ArrayList<AssertionError> errors){
        return (long node, BitSet visitedBy, BitSet seen, int iteration, MSBreadthFirst.Traveler t) -> {
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

    /**
     * Tests a MSBFS with a visitor that should visit itself and all its neighbors.
     */
    @Test
    public void testBFSVisitorNeighborsCorrect() throws IOException, InterruptedException {
        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        long[] bfsSources = generateSources( graph.getNumberOfNodes());
        ArrayList<AssertionError> errors = new ArrayList<>();

        MSBreadthFirst msbfs = new MSBreadthFirst(graph);
        msbfs.breadthFirstSearch(bfsSources,neighborsCorrect(bfsSources,graph,errors));
        msbfs.close();

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
    public MSBreadthFirst.Visitor neighborsCorrect(long[] bfsSources, IGraph graph, ArrayList<AssertionError> errors){
        BitSet[][] shouldHave = ObjectBigArrays.newBigArray(new BitSet[0][0],graph.getNumberOfNodes());
        BitSet[][] alreadySeen = ObjectBigArrays.newBigArray(new BitSet[0][0],graph.getNumberOfNodes());

        for(long node = 0; node < graph.getNumberOfNodes() ; node++) {
            ObjectBigArrays.set(shouldHave,node,new BitSet(bfsSources.length));
            ObjectBigArrays.set(alreadySeen,node,new BitSet(bfsSources.length));
        }

        return (long node, BitSet visitedBy, BitSet seen, int iteration, MSBreadthFirst.Traveler t) -> {
            synchronized (this) {
                try {
                    assertFalse("Iteration should not be more than one", iteration > 1);
                    if (iteration == 1) {
                        ObjectBigArrays.get(shouldHave,node).andNot(ObjectBigArrays.get(alreadySeen,node));
                        BitSet clonedShouldHave = (BitSet) ObjectBigArrays.get(shouldHave,node).clone();
                        clonedShouldHave.and(visitedBy);
                        assertEquals("Node " + node, clonedShouldHave.cardinality(), ObjectBigArrays.get(shouldHave,node).cardinality());

                        visitedBy.clear();
                    } else {
                        LazyLongIterator succ = graph.getSuccessors(node);
                        long out = graph.getOutdegree(node), neigh;
                        ObjectBigArrays.get(alreadySeen,node).or(visitedBy);

                        for (int neighI = 0; neighI < out; neighI++) {
                            neigh = succ.nextLong();
                            ObjectBigArrays.get(shouldHave,neigh).or(visitedBy);
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
    private long[] generateSources(long numNodes){
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        int numSources = 100;
        long[] sources = new long[numSources];
        for(int i = 0; i<numSources; i++)
            sources[i] = rand.nextLong(numNodes-1);
        return sources;

    }

    /**
     * Performs a standard BFS from each source and makes sure that the MSBFS detected the same nodes
     * @param sources
     * @param seen
     * @param graph
     */
    private void checkValidSeen(long[] sources, BitSet[][] seen, IGraph graph){
        BitSet[][] originalSeen = ObjectBigArrays.newBigArray(new BitSet[0][0],graph.getNumberOfNodes());
        BitSet temp;
        for (long i = 0; i< graph.getNumberOfNodes(); i++) {
            temp = ObjectBigArrays.get(seen, i);
            temp = temp == null ? new BitSet(sources.length) : (BitSet)temp.clone();
            ObjectBigArrays.set(originalSeen, i, temp);
        }

        /* Do standard BFS from each source */
        for (int bfs = 0; bfs < sources.length ; bfs++) {
            LongArrayFIFOQueue queue = new LongArrayFIFOQueue();
            queue.enqueue(sources[bfs]);
            LongArrayBitVector nodesChecked = LongArrayBitVector.ofLength(graph.getNumberOfNodes());
            nodesChecked.set(sources[bfs]);

            assertTrue("Source node " + sources[bfs] + " for bfs " + bfs,ObjectBigArrays.get(seen,sources[bfs]).get(bfs));

            ObjectBigArrays.get(seen,sources[bfs]).clear(bfs);

            /* Do bfs from current node */
            while (!queue.isEmpty()) {
                long curr = queue.dequeueLong();
                LazyLongIterator succs = graph.getSuccessors(curr);
                long d = graph.getOutdegree(curr);

                /* Visit all neighbors */
                while (d-- != 0) {
                    long succ = succs.nextLong();

                    if (!nodesChecked.getBoolean(succ)) {
                        nodesChecked.set(succ);
                        queue.enqueue(succ);

                        /* Make sure the MSBFS also have seen the node */
                        assertTrue("Node " + succ + " for bfs " + bfs,ObjectBigArrays.get(seen,succ).get(bfs));

                        /* Clearing the bit is used to make sure the MSBFS have not seen to many  */
                        ObjectBigArrays.get(seen,succ).clear(bfs);
                    }
                }
            }
        }

        /* If any MSBFS have any bit still set it means it have seen more than the standard BFS did */
        for(long node = 0; node<seen.length; node++){
            BitSet bitset = ObjectBigArrays.get(seen,node);
            if (bitset == null) { //Node havent been seen by any bfs
                continue;
            }
            assertEquals("Node " + node + " should be clear",0,bitset.cardinality());
        }
    }
}
