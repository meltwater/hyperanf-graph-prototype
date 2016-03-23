package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

import java.util.function.Function;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Abstract class that implements functions that
 * are common among all types of graphs.
 *
 * IGraphs purpose is to abstract what type of
 * graph the test suite is testing. In some cases
 * it is appropriate to use a physical file and in
 * other cases its not.
 */
public interface IGraph {

    IGraph copy();

    LazyLongIterator getSuccessors(long node);

    boolean addEdge(Edge edge);
    boolean addEdges(Edge ... edges);

    long getOutdegree(long node);

    NodeIterator getNodeIterator();
    NodeIterator getNodeIterator(long node);

    long getNumberOfNodes();
    long getNumberOfArcs();

    boolean containsNode(long node);

    IGraph transpose();


    /**
     * Iterates all edges in the graph and calls the lambda function for each edge.
     * The lambda function should return null if it should continue iterating
     * and a Object to indicate error. The Object will be returned.
     * @param function
     * @param <T> Return type
     * @return null if no error occurred, the error Object otherwise.
     */
    <T> T iterateAllEdges(Function<Edge, T> function);

}

