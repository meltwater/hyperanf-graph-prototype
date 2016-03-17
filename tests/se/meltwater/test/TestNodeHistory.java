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
        for (int node = 0; ; node++) {

            for (int neigh = 0; neigh < graph.getOutdegree(); neigh++)
                vc.insertEdge(new Edge(node, graph.getNextNeighbor()));

            if (node >= graph.getNumberOfNodes() - 1)
                break;
            graph.getNextNode();
        }
        NodeHistory nh = new NodeHistory(vc, 3, graph);

        HyperBoll hyperBoll = new HyperBoll(graph, 7);
        hyperBoll.init();
        for (int i = 1; i < 3; i++){
            hyperBoll.iterate();
            nh.addHistory(hyperBoll.getCounter(), i);
        }

        hyperBoll.close();

        double[] history;
        for(long node : vc.getNodesInVertexCover()){
            history = nh.count(node);
            nh.recalculateHistory(node);
            assertArrayEquals("Failed for node " + node,history,nh.count(node), 0.01);
        }

    }

}
