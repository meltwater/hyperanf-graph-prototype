package se.meltwater.examples;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.graph.Edge;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;


/**
 * Example for calculating a Vertex Cover on
 * a physical graph file.
 */

public class VertexCover {

    private DynamicVertexCover dvc;

    private NodeIterator nodeIterator;
    private long numberOfNodes = 0;

    final ImmutableGraph graph;
    private final ProgressLogger pl;
    private int progress = 0;
    private static int updateInterval = 1000000 - 1;

    public VertexCover(String graphFileName) throws IOException {
        pl = new ProgressLogger();
        graph = ImmutableGraph.loadMapped( graphFileName, pl );
        numberOfNodes = graph.numNodes();
        nodeIterator = graph.nodeIterator(0);

        pl.expectedUpdates = numberOfNodes;

        dvc = new DynamicVertexCover(new ImmutableGraphWrapper(graph));
    }

    private void fetchEdgesFromFileAndCalculateVC() throws Exception {
        pl.start();

        for( int currentNode = 0; currentNode < numberOfNodes; currentNode++ ) {
            nodeIterator.nextLong();
            LazyLongIterator successors;
            successors = nodeIterator.successors();
            long degree = nodeIterator.outdegree();

            addEdges(currentNode, successors, degree);
        }

        pl.stop();
    }

    private void addEdges(long from, LazyLongIterator successors, long numberOfSuccessors) {
        if(++progress > updateInterval) {
            pl.updateAndDisplay(progress);
            progress = 0;
        }


        if(numberOfSuccessors == 0 || dvc.isInVertexCover(from)){
            return;
        }

        long successorsLeft = numberOfSuccessors;

        while( successorsLeft != 0 ) {
            long successorOfCurrentNode = successors.nextLong();
            if(!dvc.isInVertexCover(successorOfCurrentNode)){
                dvc.insertEdge(new Edge(from, successorOfCurrentNode));
                break;
            }
            successorsLeft--;
        }
    }

    public void run () throws Exception {
        long start = System.currentTimeMillis();
        fetchEdgesFromFileAndCalculateVC();
        long end = System.currentTimeMillis();

        int maximalMatchingSize = dvc.getMaximalMatchingSize();
        int vertexCoverSize = dvc.getVertexCoverSize();
        long nodesInGraph = numberOfNodes;

        System.out.println("Maximal matching of size: " + maximalMatchingSize);
        System.out.println("Vertex cover of size: " + vertexCoverSize + " : " + nodesInGraph);
        System.out.println("Efficiency rate of " + (double)vertexCoverSize / nodesInGraph) ;
        System.out.println("Elapsed time: " + (float)(end - start)/1000 + "s");
    }
}


