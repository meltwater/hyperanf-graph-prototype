package se.meltwater.graph;

/**
 *  IGraphs purpose is to abstract what type of
 *  graph the test suite is testing. In some cases
 *  it is appropriate to use a physical file and in
 *  other cases its not.
 */
public interface IGraph {

    public void setNodeIterator(long node);
    public long  getNextNode();

    public long getNextNeighbor();
    public long getOutdegree();

    public long getNumberOfNodes();

}
