package se.meltwater;

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
    long numberOfNodes = 0;

    HashSet<Long> maximalMatching = new HashSet<>();

    public void fetchEdgesFromFile(String graphFileName) throws Exception {

        final ProgressLogger pl = new ProgressLogger();
        final ImmutableGraph graph = ImmutableGraph.load( graphFileName, pl );
        numberOfNodes = graph.numNodes();

        NodeIterator nodeIterator = graph.nodeIterator(0);

        for( int currentNode = 0; currentNode < numberOfNodes; currentNode++ ) {
            nodeIterator.nextLong();
            LazyLongIterator successors;
            successors = nodeIterator.successors();
            long degree = nodeIterator.outdegree();

            addEdges(currentNode, successors, degree);
        }
    }

    private void addEdges(long from, LazyLongIterator successors, long numberOfSuccessors) {
        if(numberOfSuccessors == 0) {
            return;
        }

        if(maximalMatching.contains(from)) {
            return;
        }

        long successorsLeft = numberOfSuccessors;

        while( successorsLeft != 0 ) {
            long successorOfCurrentNode = successors.nextLong();
            if(!maximalMatching.contains(successorOfCurrentNode)) {
                maximalMatching.add(from);
                maximalMatching.add(successorOfCurrentNode);
            }
            successorsLeft--;
        }
    }


    public static void main(String[] args) throws Exception {
        String graphFileName = "/home/johan/programming/master/it/unimi/dsi/webgraph/graphs/indochina-2004";

        VertexCover vertexCover = new VertexCover();
        vertexCover.fetchEdgesFromFile(graphFileName);

        int matchingSize = vertexCover.maximalMatching.size();
        long nodesInGraph = vertexCover.numberOfNodes;


        System.out.println("Vertex cover of size: " + matchingSize
                + " and originally was: " + nodesInGraph + " giving a efficiency rate of " +
                (double)matchingSize / nodesInGraph) ;

    }
}
