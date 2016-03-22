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
 */
public abstract class AGraph implements IGraph {

    @Override
    abstract public IGraph copy();

    @Override
    abstract public LazyLongIterator getSuccessors(long node);

    @Override
    abstract public boolean addEdges(Edge ... edges);

    @Override
    abstract public boolean addEdge(Edge edge);

    @Override
    abstract public long getOutdegree(long node);

    @Override
    abstract public NodeIterator getNodeIterator();
    @Override
    abstract public NodeIterator getNodeIterator(long node);

    @Override
    abstract public long getNumberOfNodes();
    @Override
    abstract public long getNumberOfArcs();

    @Override
    public boolean containsNode(long node) {
        return node < getNumberOfNodes();
    }

    /**
     * Iterates all edges in the graph and calls the lambda function for each edge.
     * The lambda function should return null if it should continue iterating
     * and a Object to indicate error. The Object will be returned.
     * @param function
     * @param <T> Return type
     * @return null if no error occurred, the error Object otherwise.
     */
    @Override
    public <T> T iterateAllEdges(Function<Edge, T> function) {
        NodeIterator nodeIt = getNodeIterator();
        long numNodes = getNumberOfNodes();
        for(long node = 0; node < numNodes; node++) {
            nodeIt.nextLong();
            long outdegree = nodeIt.outdegree();
            LazyLongIterator neighbors = nodeIt.successors();

            while(outdegree != 0) {
                long neighbor = neighbors.nextLong();
                T returnedValue = function.apply(new Edge(node, neighbor));
                if(returnedValue != null) {
                    return returnedValue;
                }

                outdegree--;
            }
        }
        return null;
    }




}
