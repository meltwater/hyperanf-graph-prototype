package se.meltwater.graph;

import java.util.*;

/**
 * Created by johan on 2016-02-29.
 */
public class SimulatedGraph implements IGraph {


    private int nodeIterator = 0;
    private Iterator<Integer> successors;
    private TreeMap<Integer, HashSet<Integer>> iteratorNeighbors = new TreeMap<>();

    public ArrayList<Node> nodes = new ArrayList<>();
    public ArrayList<Edge> edges = new ArrayList<>();

    public void addNode(Node node) {
        iteratorNeighbors.put(node.id, new HashSet<>());
        nodes.add(node);
    }

    public void addEdge(Edge edge){
        HashSet neighbors = iteratorNeighbors.get(edge.from.id);
        neighbors.add(edge.to.id);
        edges.add(edge);
    }

    public void removeEdge(Edge edge) {
        HashSet neighbors = iteratorNeighbors.get(edge.from.id);
        neighbors.remove(edge.to.id);
        edges.remove(edge);

    }

    @Override
    public void setNodeIterator(int node) {
        nodeIterator = node;
        successors = iteratorNeighbors.get(node).iterator();
    }

    @Override
    public int getNextNode() {
        int nextNode = iteratorNeighbors.higherKey(nodeIterator);
        setNodeIterator(nextNode);
        return nextNode;
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
