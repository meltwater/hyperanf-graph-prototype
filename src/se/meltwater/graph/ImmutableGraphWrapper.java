package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

/**
 * Wraps an Immutable graph to be able to use it
 * with the IGraph interface. The IGraph interface
 * is useful in testing as the type of the graph is now
 * abstracted away from the test cases.
 */
public class ImmutableGraphWrapper extends IGraph{

    private ImmutableGraph graph;
    private NodeIterator nodeIterator;
    LazyLongIterator successors;


    public ImmutableGraphWrapper(ImmutableGraph graph) {
        this.graph = graph;
    }

    @Override
    public void setNodeIterator(long node) {
        nodeIterator = graph.nodeIterator(node);
        nodeIterator.nextLong();
        successors = nodeIterator.successors();
    }

    @Override
    public long getNextNode() {
        return nodeIterator.nextLong();
    }

    @Override
    public long getNextNeighbor() {
        return successors.nextLong();
    }

    @Override
    public long getOutdegree() {
        return nodeIterator.outdegree();
    }

    @Override
    public long getNumberOfNodes() {
        return graph.numNodes();
    }

    @Override
    public long getNumberOfArcs() { return graph.numArcs(); }

    public NodeIterator getNodeIterator(long node){
        return graph.nodeIterator(node);
    }

    public NodeIterator getNodeIterator(){
        return getNodeIterator(0);
    }
}
