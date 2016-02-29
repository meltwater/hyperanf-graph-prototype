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
 * Created by johan on 2016-02-23.
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


    public static void main(String[] args) throws Exception {

        //String graphFileName = "/home/johan/programming/master/it/unimi/dsi/webgraph/graphs/uk-2002";
        SimpleJSAP jsap = new SimpleJSAP( VertexCover.class.getName(), "Calculates a vertex cover from a basefile of a graph",
                new Parameter[] {
                        new FlaggedOption( "path", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'p', "path", "" ),
                }
        );

        JSAPResult jsapResult = jsap.parse( args );
        if ( jsap.messagePrinted() ) System.exit( 1 );

        String graphFileName = jsapResult.getString( "path" );

        VertexCover vertexCover = new VertexCover(graphFileName);

        long start = System.currentTimeMillis();
        vertexCover.fetchEdgesFromFileAndCalculateVC();
        long end = System.currentTimeMillis();

        int maximalMatchingSize = vertexCover.dvc.getMaximalMatchingSize();
        int vertexCoverSize = vertexCover.dvc.getVertexCoverSize();
        long nodesInGraph = vertexCover.numberOfNodes;

        System.out.println("Maximal matching of size: " + maximalMatchingSize);
        System.out.println("Vertex cover of size: " + vertexCoverSize + " : " + nodesInGraph);
        System.out.println("Efficiency rate of " + (double)vertexCoverSize / nodesInGraph) ;
        System.out.println("Elapsed time: " + (float)(end - start)/1000 + "s");
    }
}

