package se.meltwater.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by johan on 2016-02-29.
 */
public class SimulatedGraph implements IGraph {


    private int nodeIterator = 0;
    private Iterator<Integer> successors;
    private HashMap<Integer, HashSet<Integer>> iteratorNeighbors = new HashMap<>();

    public void addNode(Node node) {
        iteratorNeighbors.put(node.id, new HashSet<>());
    }

    public void addEdge(Edge edge){
        HashSet neighbors = iteratorNeighbors.get(edge.from.id);
        neighbors.add(edge.to.id);
    }

    public void removeEdge(Edge edge) {
        HashSet neighbors = iteratorNeighbors.get(edge.from.id);
        neighbors.remove(edge.to.id);
    }

    @Override
    public void setNodeIterator(int node) {
        nodeIterator = node;
        successors = iteratorNeighbors.get(node).iterator();
    }

    @Override
    public int getNextNode() {
        return 0;
    }

    @Override
    public int getNextNeighbor() {
        nodeIterator++;
        return successors.next();
    }

    @Override
    public int getOutdegree() {
        return iteratorNeighbors.get(nodeIterator).size();
    }
}
