import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.UnionImmutableGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.GraphChanger;

import java.io.File;
import java.io.IOException;

/**
 * Created by simon on 2016-02-26.
 */
public class TestGraphChanger extends GraphChanger{

    @Test
    public void testMerge(){
        String merge1 = "testGraphs/noBlocksUk", merge2 = "testGraphs/wordassociationNoBlocks", temp = "testGraphs/tempOutGraph";
        try {
            merge(merge1,merge2,temp);
            BVGraph mergedGraph = BVGraph.loadMapped(temp);
            UnionImmutableGraph unionedGraph = new UnionImmutableGraph(BVGraph.loadMapped(merge1),BVGraph.loadMapped(merge2));
            assertEquals(mergedGraph.numNodes(),unionedGraph.numNodes());
            NodeIterator nodeItMerged = mergedGraph.nodeIterator();
            NodeIterator nodeItUnioned = unionedGraph.nodeIterator();

            for(long node = 0; node < mergedGraph.numNodes() ; node++){
                nodeItMerged.nextLong(); nodeItUnioned.nextLong();
                long outM = nodeItMerged.outdegree(), outU = nodeItUnioned.outdegree();
                assertEquals("Node " + node + " didn't match",outM,outU);
                if(outM == 0 || outM != outU)
                    continue;
                LazyLongIterator succItM = nodeItMerged.successors(), succItU = nodeItUnioned.successors();
                for(int succI = 0; succI < outM; succI++){
                    assertEquals("Successor at index " + succI + " for node " + node,succItM.nextLong(),succItU.nextLong());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            assertFalse(true);
        }

        File tempOut;
        for(String fileExtension : new String[]{".properties",".graph",".offsets"}){
            tempOut = new File(temp + fileExtension);
            if(tempOut.exists())
                tempOut.delete();
        }

    }

}
