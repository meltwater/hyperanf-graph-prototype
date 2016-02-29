package se.meltwater.graph;

import java.util.*;

/**
 * Created by johan on 2016-02-29.
 */
public interface IGraph {

    public void setNodeIterator(int node);
    public int getNextNode();

    public int getNextNeighbor();
    public int getOutdegree();

}
