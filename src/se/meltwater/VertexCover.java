package se.meltwater;

import com.martiansoftware.jsap.*;
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
    private final ProgressLogger pl;

    private int progress = 0;

    private static int updateInterval = 1000000;

    public VertexCover(String graphFileName) throws IOException {
        pl = new ProgressLogger();
        final ImmutableGraph graph = ImmutableGraph.loadMapped( graphFileName, pl );
        numberOfNodes = graph.numNodes();
        nodeIterator = graph.nodeIterator(0);

        pl.expectedUpdates = numberOfNodes;
    }

    public void fetchEdgesFromFileAndCalculateVC() throws Exception {
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
        if(progress > updateInterval) {
            pl.updateAndDisplay(progress);
            progress = 0;
        }
        progress++;


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
        /*SimpleJSAP jsap = new SimpleJSAP( VertexCover.class.getName(), "Compresses differentially a graph. Source and destination are basenames from which suitable filenames will be stemmed; alternatively, if the suitable option was specified, source is a spec (see below). For more information about the compression techniques, see the Javadoc documentation.",
                new Parameter[] {
                        new FlaggedOption( "path", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'p', "path", "" ),
                }
        );

        JSAPResult jsapResult = jsap.parse( args );
        if ( jsap.messagePrinted() ) System.exit( 1 );

        String graphFileName = jsapResult.getString( "path" );*/

        VertexCover vertexCover = new VertexCover(graphFileName);

        long start = System.currentTimeMillis();
        vertexCover.fetchEdgesFromFileAndCalculateVC();
        long end = System.currentTimeMillis();

        int maximalMatchingSize = vertexCover.maximalMatching.size();
        int vertexCoverSize = vertexCover.vertexCover.cardinality();
        long nodesInGraph = vertexCover.numberOfNodes;

        System.out.println("Maximal matching of size: " + maximalMatchingSize);
        System.out.println("Vertex cover of size: " + vertexCoverSize + " : " + nodesInGraph);
        System.out.println("Efficiency rate of " + (double)vertexCoverSize / nodesInGraph) ;
        System.out.println("Elapsed time: " + (float)(end - start)/1000 + "s");
    }
}


