package it.unimi.dsi.big.webgraph.history;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.TestUtils;
import org.junit.Test;
import it.unimi.dsi.big.webgraph.algo.DANF;
import it.unimi.dsi.big.webgraph.algo.TrivialDynamicANF;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.SimulatedGraph;
import static org.junit.Assert.*;

import java.util.Random;

/**
 * TODO Class description
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class TrivialAlgTest {

    private int maxIterations = 100;
    private int maxNodes = 100;
    private double epsilon = 0.1;
    private double additionalNodes = 1.1;
    private int edgeAdditionsForEachIteration = 5;

    @Test
    public void testTrivialSameAsDANF() throws InterruptedException {
        Random rand = new Random();
        for(int iteration = 0; iteration < maxIterations; iteration++){
            SimulatedGraph g1 = TestUtils.genRandomGraph(maxNodes);
            SimulatedGraph g2 = (SimulatedGraph) g1.clone();

            int h = rand.nextInt(6)+1;
            int log2m = rand.nextInt(4)+4;

            long seed = Util.randomSeed();
            DANF danf = new DANF(h,log2m,g1,seed);
            TrivialDynamicANF tanf = new TrivialDynamicANF(h,log2m,g2,seed);

            assertSame(danf,tanf);

            for (int i = 0; i < edgeAdditionsForEachIteration; i++) {
                Edge[] extraEdges = TestUtils.generateEdges((int)(g1.numNodes()*additionalNodes),(int)g1.numNodes());
                danf.addEdges(extraEdges);
                tanf.addEdges(extraEdges);
                assertSame(danf,tanf);
            }

        }
    }

    private void assertSame(DANF danf, TrivialDynamicANF tanf) {
        assertEquals(danf.getGraph().numNodes(),tanf.getGraph().numNodes());
        for(long node = 0; node < danf.getGraph().numNodes(); node++){
            assertEquals(danf.count(node,danf.getMaxH()),tanf.count(node),epsilon);
        }
    }

}
