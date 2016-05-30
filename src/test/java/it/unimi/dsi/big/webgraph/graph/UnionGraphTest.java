package it.unimi.dsi.big.webgraph.graph;

import it.unimi.dsi.big.webgraph.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Tests for the Unioned graph class
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class UnionGraphTest {

    /**
     * Tests that the outdegrees of a unioned graph is correct.
     * @throws IOException
     */
    @Test
    public void testUnionOutdegree() throws IOException {
        MutableGraph graph = new ImmutableGraphWrapper(BVGraph.loadMapped("testGraphs/noBlocksUk"));
        long numArcsAtStart = graph.numArcs();

        SimulatedGraph graphToUnion = TestUtils.genRandomGraph(1000);
        graph.addEdges(graphToUnion.getAllEdges()); //Graph will now be unioned internally

        long currentNrArcs = 0;

        for (long i = 0; i < graph.numNodes(); i++) {
            currentNrArcs += graph.outdegree(i);
        }

        assertTrue(currentNrArcs > numArcsAtStart);
    }
}
