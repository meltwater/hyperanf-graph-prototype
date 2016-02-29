package se.meltwater;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.graph.Edge;
import se.meltwater.graph.ImmutableGraphWrapper;
import se.meltwater.graph.Node;
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


        if(numberOfSuccessors == 0 || dvc.isInVertexCover(new Node((int)from))){
            return;
        }

        long successorsLeft = numberOfSuccessors;

        while( successorsLeft != 0 ) {
            long successorOfCurrentNode = successors.nextLong();
            if(!dvc.isInVertexCover(new Node((int)successorOfCurrentNode))){
                dvc.insertEdge(new Edge(new Node((int)from), new Node((int)successorOfCurrentNode)));
                break;
            }
            successorsLeft--;
        }
    }


    public boolean isVertexCover() {
        nodeIterator = graph.nodeIterator(0);

        for( int currentNode = 0; currentNode < numberOfNodes; currentNode++ ) {
            nodeIterator.nextLong();
            LazyLongIterator successors;
            successors = nodeIterator.successors();
            long degree = nodeIterator.outdegree();

            if(degree == 0 || dvc.isInVertexCover(new Node((currentNode)))){
                continue;
            }

            while( degree != 0 ) {
                long successorOfCurrentNode = successors.nextLong();
                if(!dvc.isInVertexCover(new Node((int)successorOfCurrentNode))){
                    return false;
                }
                degree--;
            }
        }

        return true;
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

        int maximalMatchingSize = vertexCover.dvc.getMaximalMatchingSize();
        int vertexCoverSize = vertexCover.dvc.getVertexCoverSize();
        long nodesInGraph = vertexCover.numberOfNodes;

        System.out.println("Maximal matching of size: " + maximalMatchingSize);
        System.out.println("Vertex cover of size: " + vertexCoverSize + " : " + nodesInGraph);
        System.out.println("Efficiency rate of " + (double)vertexCoverSize / nodesInGraph) ;
        System.out.println("Elapsed time: " + (float)(end - start)/1000 + "s");

        System.out.println("Checking that solution really is a vertex cover.");
        System.out.println("The calculated VC is a vertex cover: " + vertexCover.isVertexCover());

        System.out.println("Completed!");
    }
}


