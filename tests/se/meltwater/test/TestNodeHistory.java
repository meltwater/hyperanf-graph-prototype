package se.meltwater.test;

import it.unimi.dsi.big.webgraph.BVGraph;
import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.NodeHistory;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.io.IOException;

/**
 * Created by simon on 2016-03-16.
 */
public class TestNodeHistory {

    @Test
    public void historyUnchangedOnRecalculation() throws IOException, InterruptedException {

        BVGraph bvGraph = BVGraph.load("testGraphs/wordassociationNoBlocks");
        IGraph graph = new ImmutableGraphWrapper(bvGraph);
        IDynamicVertexCover vc = new DynamicVertexCover(graph);
        graph.setNodeIterator(0);
        for(int node = 0; ; node++){

            for (int neigh = 0; neigh < graph.getOutdegree(); neigh++)
                vc.insertEdge(new Edge(node,graph.getNextNeighbor()));

            if(node >= graph.getNumberOfNodes()-1)
                break;
            graph.getNextNode();
        }
        NodeHistory nh = new NodeHistory(vc,2,graph);

        HyperBoll hyperBoll = new HyperBoll(bvGraph,7);
        hyperBoll.init();
        hyperBoll.iterate();
        nh.addHistory(hyperBoll.getCounter(),1);

        hyperBoll.close();

        double history;
        for(long node : vc.getNodesInVertexCover()){
            history = nh.count(node,1);
            nh.recalculateHistory(node);
            assertEquals("Failed for node " + node,history,nh.count(node,1), 0.01);
        }

    }

}
