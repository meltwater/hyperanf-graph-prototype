package se.meltwater.graph;

import java.util.*;

/**
 * Created by johan on 2016-02-29.
 */
public class SimulatedGraph implements IGraph {


    private long nodeIterator = 0;
    private Iterator<Long> successors;
    private TreeMap<Long, HashSet<Long>> iteratorNeighbors = new TreeMap<>();

    public ArrayList<Long> nodes = new ArrayList<>();
    public ArrayList<Edge> edges = new ArrayList<>();

    public void addNode(long node) {
        iteratorNeighbors.put(node, new HashSet<>());
        nodes.add(node);
    }

    public void addEdge(Edge edge){
        HashSet neighbors = iteratorNeighbors.get(edge.from);
        neighbors.add(edge.to);
        edges.add(edge);
    }

    public void removeEdge(Edge edge) {
        HashSet neighbors = iteratorNeighbors.get(edge.from);
        neighbors.remove(edge.to);
        edges.remove(edge);

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
    public long getNextNeighbor() {
        nodeIterator++;
        return successors.next();
    }

    @Override
    public long getOutdegree() {
        return iteratorNeighbors.get(nodeIterator).size();
    }

    @Override
    public long getNumberOfNodes() {
        return nodes.size();
    }
}
