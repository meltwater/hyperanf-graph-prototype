package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.NodeIterator;

import java.util.*;

/**
 * Created by johan on 2016-02-29.
 */
public interface IGraph {

    void setNodeIterator(long node);
    long  getNextNode();

    long getNextNeighbor();
    long getOutdegree();

    NodeIterator getNodeIterator();
    NodeIterator getNodeIterator(long node);

    long getNumberOfNodes();

}
