package se.meltwater.test;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.algo.HyperBall;
import org.junit.Test;
import se.meltwater.algo.HyperBoll;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by simon on 2016-03-16.
 */
public class TestHyperBoll {

    @Test
    public void testHyperBollSameAsHyperBall() throws IOException {
        BVGraph graph = BVGraph.load("testGraphs/wordassociationNoBlocks");
        long seed = Util.randomSeed();

        HyperBall hyperBall = new HyperBall(graph,7,seed);
        HyperBoll hyperBoll = new HyperBoll(graph,7,seed);
        hyperBall.init();
        hyperBoll.init();
        hyperBall.run();
        hyperBoll.run();
        hyperBall.close();
        hyperBoll.close();

        HyperLolLolCounterArray hyperBollCounter = hyperBoll.getCounter();
        for(int node=0; node < graph.numNodes(); node++)
            assertEquals(hyperBall.count(node),hyperBollCounter.count(node), 0.01);
    }

}
