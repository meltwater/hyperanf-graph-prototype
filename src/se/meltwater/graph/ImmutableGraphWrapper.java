package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Wraps an Immutable graph to be able to use it
 * with the IGraph interface. The IGraph interface
 * is useful in testing as the type of the graph is now
 * abstracted away from the test cases.
 */
public class ImmutableGraphWrapper extends AGraph{

    private ImmutableGraph graph;
    private NodeIterator nodeIterator;
    LazyLongIterator successors;


    public ImmutableGraphWrapper(ImmutableGraph graph) {
        this.graph = graph;
    }

    @Override
    public IGraph copy(){
        return new ImmutableGraphWrapper(graph.copy());
    }

    @Override
    public long getOutdegree(long node){
        return graph.outdegree(node);
    }

    @Override
    public LazyLongIterator getSuccessors(long node){
        return graph.successors(node);
    }

    @Override
    public long getNumberOfNodes() {
        return graph.numNodes();
    }

    @Override
    public long getNumberOfArcs() { return graph.numArcs(); }

    @Override
    public NodeIterator getNodeIterator(long node){
        return graph.nodeIterator(node);
    }

    @Override
    public NodeIterator getNodeIterator(){
        return getNodeIterator(0);
    }
}
