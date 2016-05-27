package se.meltwater.examples;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;


/**
 *
 * Example for calculating a Vertex Cover on
 * a physical graph file.
 *
 * Usage:
 * <pre>
 *     {@code
 *     VertexCoverExample vce = new VertexCoverExample(graphBaseName);
 *     vce.run();
  *     }
 * </pre>
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */

public class VertexCoverExample {

    private DynamicVertexCover dvc;
    private long numberOfNodes = 0;
    final IGraph graph;

    private final ProgressLogger pl;
    private int progress = 0;
    private static int updateInterval = 1000000 - 1;

    /**
     * Initiates the vertex cover example
     * @param graphFileName The path and basename of the graph file
     * @throws IOException
     */
    public VertexCoverExample(String graphFileName) throws IOException {
        pl = new ProgressLogger();
        graph = new ImmutableGraphWrapper(ImmutableGraph.loadMapped( graphFileName, pl ));
        numberOfNodes = graph.getNumberOfNodes();

        pl.expectedUpdates = graph.getNumberOfArcs();

        dvc = new DynamicVertexCover(graph);
    }

    /**
     * Adds all graph edges to the vertex cover
     * @throws Exception
     */
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

    /**
     * Runs and prints statistics of the calculated Vertex cover.
     * @throws Exception
     */
    public void run () throws Exception {
        long start = System.currentTimeMillis();
        fetchEdgesFromFileAndCalculateVC();
        long end = System.currentTimeMillis();

        long maximalMatchingSize = dvc.getMaximalMatchingSize();
        long vertexCoverSize = dvc.getVertexCoverSize();
        long nodesInGraph = numberOfNodes;

        System.out.println("Maximal matching of size: " + maximalMatchingSize);
        System.out.println("Vertex cover of size: " + vertexCoverSize + " : " + nodesInGraph);
        System.out.println("Efficiency rate of " + (double)vertexCoverSize / nodesInGraph) ;
        System.out.println("Elapsed time: " + (float)(end - start)/1000 + "s");
    }
}


