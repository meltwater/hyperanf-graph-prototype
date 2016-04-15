package se.meltwater.test;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.algo.HyperBall;
import org.junit.Test;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Created by simon on 2016-03-16.
 */
public class TestHyperBoll {

    @Test
    public void testHyperBollSameAsHyperBall() throws IOException {
        BVGraph graph = BVGraph.load("testGraphs/wordassociationNoBlocks");
        long seed = Util.randomSeed();
        int h = new Random().nextInt(5)+1;

        HyperBall hyperBall = new HyperBall(graph,7,seed);
        HyperBoll hyperBoll = new HyperBoll(new ImmutableGraphWrapper(graph),7,seed);
        hyperBall.init();
        hyperBoll.init();
        for (int i = 0; i < h ; i++) {
            hyperBall.iterate();
            hyperBoll.iterate();
        }
        hyperBall.close();
        hyperBoll.close();

        HyperLolLolCounterArray hyperBollCounter = hyperBoll.getCounter();
        for(int node=0; node < graph.numNodes(); node++)
            assertEquals(hyperBall.count(node),hyperBollCounter.count(node), 0.01);
    }

}
