package se.meltwater.graph;

import java.util.*;

/**
 * Instead of loading a physical graph file
 * this class can be used to simulate a graph.
 * Mainly used for testing purposes when it's
 * not feasable to create a physical file for
 * each test case.
 */
public class SimulatedGraph implements IGraph {


    private long nodeIterator = 0;
    private Iterator<Long> successors;
    private TreeMap<Long, HashSet<Long>> iteratorNeighbors = new TreeMap<>();


    public void addNode(long node) {
        iteratorNeighbors.put(node, new HashSet<>());
    }

    public void addEdge(Edge edge){
        HashSet neighbors = iteratorNeighbors.get(edge.from);
        neighbors.add(edge.to);
    }

    public void deleteEdge(Edge edge) {
        HashSet neighbors = iteratorNeighbors.get(edge.from);
        neighbors.remove(edge.to);
    }

    @Override
    public void setNodeIterator(long node) {
        nodeIterator = node;
        successors = iteratorNeighbors.get(node).iterator();
    }

    @Override
    public long getNextNode() {
        long nextNode = iteratorNeighbors.higherKey(nodeIterator);
        setNodeIterator(nextNode);
        return nextNode;
    }

    @Override
    public long getNextNeighbor() throws NoSuchElementException{
        return successors.next();
    }

    @Override
    public long getOutdegree() {
        return iteratorNeighbors.get(nodeIterator).size();
    }

    @Override
    public long getNumberOfNodes() {
        return iteratorNeighbors.size();
    }

}
