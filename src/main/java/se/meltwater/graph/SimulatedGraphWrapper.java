package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

/**
 * // TODO Class description
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class SimulatedGraphWrapper extends ImmutableGraph{

    SimulatedGraph graph;

    public SimulatedGraphWrapper(SimulatedGraph graph){
        this.graph = graph;
    }

    @Override
    public long numArcs() {
        return graph.getNumberOfArcs();
    }

    @Override
    public LazyLongIterator successors(long x) {
        return graph.getSuccessors(x);
    }

    @Override
    public NodeIterator nodeIterator(long from) {
        return graph.getNodeIterator(from);
    }

    @Override
    public long numNodes() {
        return graph.getNumberOfNodes();
    }

    @Override
    public boolean randomAccess() {
        return true;
    }

    @Override
    public long outdegree(long l) {
        return graph.getOutdegree(l);
    }

    @Override
    public ImmutableGraph copy() {
        return new SimulatedGraphWrapper((SimulatedGraph) graph.copy());
    }
}

