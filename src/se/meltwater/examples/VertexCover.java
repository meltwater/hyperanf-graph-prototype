package se.meltwater.examples;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;


/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Example for calculating a Vertex Cover on
 * a physical graph file.
 */

public class VertexCover {

    private DynamicVertexCover dvc;

    private long numberOfNodes = 0;

    final IGraph graph;

    private final ProgressLogger pl;
    private int progress = 0;
    private static int updateInterval = 1000000 - 1;

    public VertexCover(String graphFileName) throws IOException {
        pl = new ProgressLogger();
        graph = new ImmutableGraphWrapper(ImmutableGraph.loadMapped( graphFileName, pl ));
        numberOfNodes = graph.getNumberOfNodes();

        pl.expectedUpdates = graph.getNumberOfArcs();

        dvc = new DynamicVertexCover(graph);
    }

    private void fetchEdgesFromFileAndCalculateVC() throws Exception {
        pl.start();

        graph.iterateAllEdges(edge -> {
            if(++progress > updateInterval) {
                pl.updateAndDisplay(progress);
                progress = 0;
            }

            dvc.insertEdge(edge);
            return null;
        });

        pl.stop();
    }

    public void run () throws Exception {
        long start = System.currentTimeMillis();
        fetchEdgesFromFileAndCalculateVC();
        long end = System.currentTimeMillis();

        int maximalMatchingSize = dvc.getMaximalMatchingSize();
        long vertexCoverSize = dvc.getVertexCoverSize();
        long nodesInGraph = numberOfNodes;

        System.out.println("Maximal matching of size: " + maximalMatchingSize);
        System.out.println("Vertex cover of size: " + vertexCoverSize + " : " + nodesInGraph);
        System.out.println("Efficiency rate of " + (double)vertexCoverSize / nodesInGraph) ;
        System.out.println("Elapsed time: " + (float)(end - start)/1000 + "s");
    }

}


