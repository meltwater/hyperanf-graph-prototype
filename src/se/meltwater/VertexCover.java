package se.meltwater;

import com.sun.org.apache.xpath.internal.SourceTree;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyIntIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.util.*;

/**
 * Created by johan on 2016-02-23.
 */

public class VertexCover {

    private NodeIterator nodeIterator;
    private long numberOfNodes = 0;

    private HashMap<Integer, Integer> maximalMatching = new HashMap<>();
    private BitSet vertexCover = new BitSet();

    public VertexCover(String graphFileName) throws IOException {
        final ProgressLogger pl = new ProgressLogger();
        final ImmutableGraph graph = ImmutableGraph.load( graphFileName, pl );
        numberOfNodes = graph.numNodes();

        nodeIterator = graph.nodeIterator(0);
    }

    public void fetchEdgesFromFile() throws Exception {
        for( int currentNode = 0; currentNode < numberOfNodes; currentNode++ ) {
            nodeIterator.nextLong();
            LazyLongIterator successors;
            successors = nodeIterator.successors();
            long degree = nodeIterator.outdegree();

            addEdges(currentNode, successors, degree);
        }
    }

    private void addEdges(long from, LazyLongIterator successors, long numberOfSuccessors) {
        if(numberOfSuccessors == 0 || vertexCover.get((int)from)) {
            return;
        }

        long successorsLeft = numberOfSuccessors;

        while( successorsLeft != 0 ) {
            long successorOfCurrentNode = successors.nextLong();
            if(!vertexCover.get((int)successorOfCurrentNode)) {
                maximalMatching.put((int)from, (int)successorOfCurrentNode);

                vertexCover.set((int)from);
                vertexCover.set((int)successorOfCurrentNode);

                break;
            }
            successorsLeft--;
        }
    }


    public static void main(String[] args) throws Exception {
        String graphFileName = "/home/johan/programming/master/it/unimi/dsi/webgraph/graphs/uk-2002";

        VertexCover vertexCover = new VertexCover(graphFileName);

        long start = System.currentTimeMillis();
        vertexCover.fetchEdgesFromFile();
        long end = System.currentTimeMillis();

        int maximalMatchingSize = vertexCover.maximalMatching.size();
        int vertexCoverSize = vertexCover.vertexCover.cardinality();
        long nodesInGraph = vertexCover.numberOfNodes;

        System.out.println("Maximal matching of size: " + maximalMatchingSize);
        System.out.println("Vertex cover of size: " + vertexCoverSize
                + " and originally was: " + nodesInGraph);
        System.out.println(" giving a efficiency rate of " +
                (double)vertexCoverSize / nodesInGraph) ;
        System.out.println("Elapsed time: " + (float)(end - start)/1000);
    }
}


