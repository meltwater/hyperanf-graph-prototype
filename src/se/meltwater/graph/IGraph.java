package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

/**
 *  IGraphs purpose is to abstract what type of
 *  graph the test suite is testing. In some cases
 *  it is appropriate to use a physical file and in
 *  other cases its not.
 */
public interface IGraph {

    void setNodeIterator(long node);
    long  getNextNode();

    long getNextNeighbor();
    long getOutdegree();
    long getOutdegree(long node);

    IGraph copy();

    NodeIterator getNodeIterator();
    NodeIterator getNodeIterator(long node);

    LazyLongIterator getSuccessors(long node);

    long getNumberOfNodes();
    long getNumberOfArcs();

}
