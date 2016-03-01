package se.meltwater.graph;

import java.util.*;

/**
 * Created by johan on 2016-02-29.
 */
public interface IGraph {

    public void setNodeIterator(long node);
    public long  getNextNode();

    public long getNextNeighbor();
    public long getOutdegree();

    public long getNumberOfNodes();

}
