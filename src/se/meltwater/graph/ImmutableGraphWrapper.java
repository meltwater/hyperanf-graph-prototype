package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

/**
 * Created by johan on 2016-02-29.
 */
public class ImmutableGraphWrapper implements IGraph{

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
}
