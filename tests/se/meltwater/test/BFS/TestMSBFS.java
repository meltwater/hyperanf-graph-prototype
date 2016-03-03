package se.meltwater.test.BFS;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyIntIterator;
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
 * Created by simon on 2016-03-01.
 */
public class TestMSBFS {

    @Test
    public void testOnGeneratedGraph() throws InterruptedException {
        SimulatedGraph graph = TestUtils.genRandomGraph(1000);
        testGraph(graph);
    }

    @Test
    public void testBFSValidity() throws IOException, InterruptedException {

        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        testGraph(graph);

    }

    public void testGraph(IGraph graph) throws InterruptedException {

        int[] bfsSources = generateSources((int) graph.getNumberOfNodes());
        MSBreadthFirst msbfs = new MSBreadthFirst(bfsSources, graph);
        BitSet[] seen = msbfs.breadthFirstSearch();
        checkValidSeen(bfsSources, seen, graph);

    }

    @Test
    public void testBFSVisitorOnlySources() throws IOException, InterruptedException {

        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        int[] bfsSources = generateSources((int) graph.getNumberOfNodes());
        MSBreadthFirst msbfs = new MSBreadthFirst(bfsSources, graph, onlySourcesVisitor(bfsSources));
        msbfs.breadthFirstSearch();

    }

    public MSBreadthFirst.Visitor onlySourcesVisitor(int[] bfsSources){
        return (long node, BitSet visitedBy, BitSet seen, int iteration) -> {
            assertEquals("Iteration for node " + node + " should be zero",0,iteration);
            int bfs = -1;
            while((bfs = visitedBy.nextSetBit(bfs+1)) != -1)
                assertEquals("Node " + node + " should be visited by " + bfs,bfsSources[bfs], node);
            visitedBy.clear();
        };
    }

    @Test
    public void testBFSVisitorNeighborsCorrect() throws IOException, InterruptedException {

        IGraph graph = new ImmutableGraphWrapper(BVGraph.load("testGraphs/noBlocksUk"));
        int[] bfsSources = generateSources((int) graph.getNumberOfNodes());
        ArrayList<AssertionError> errors = new ArrayList<>();
        MSBreadthFirst msbfs = new MSBreadthFirst(bfsSources, graph,neighborsCorrect(bfsSources,graph,errors));
        msbfs.breadthFirstSearch();
        assertEquals(0,errors.size());
    }

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

    private int[] generateSources(int numNodes){

        Random rand = new Random(System.currentTimeMillis());
        int numSources = 100;
        int[] sources = new int[numSources];
        for(int i = 0; i<numSources; i++)
            sources[i] = rand.nextInt(numNodes-1);
        return sources;

    }

    private void checkValidSeen(int[] sources, BitSet[] seen, IGraph graph){

        BitSet[] originalSeen = new BitSet[seen.length];
        int i = 0;
        for(BitSet bits : seen)
            originalSeen[i++] = (BitSet)bits.clone();
        for(int bfs = 0; bfs < sources.length ; bfs++){

            IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
            queue.enqueue(sources[bfs]);
            BitSet nodesChecked = new BitSet((int)graph.getNumberOfNodes());
            nodesChecked.set(sources[bfs]);
            assertTrue("Source node " + sources[bfs] + " for bfs " + bfs,seen[sources[bfs]].get(bfs));
            seen[sources[bfs]].clear(bfs);
            while (!queue.isEmpty()) {
                int curr = queue.dequeueInt();
                graph.setNodeIterator(curr);
                int d = (int)graph.getOutdegree();

                while (d-- != 0) {
                    int succ = (int)graph.getNextNeighbor();
                    if (!nodesChecked.get(succ)) {
                        nodesChecked.set(succ);
                        queue.enqueue(succ);
                        assertTrue("Node " + succ + " for bfs " + bfs,seen[succ].get(bfs));
                        seen[succ].clear(bfs);
                    }
                }
            }

        }

        for(int node = 0; node<seen.length; node++){
            assertEquals("Node " + node + " should be clear",0,seen[node].cardinality());
        }

    }

}
