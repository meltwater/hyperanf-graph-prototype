package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.utils.Utils;

import java.io.IOException;
import java.util.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Instead of loading a physical graph file
 * this class can be used to simulate a graph.
 * Mainly used for testing purposes when it's
 * not feasable to create a physical file for
 * each test case.
 */
public class SimulatedGraph extends AGraph implements  Cloneable {

    private TreeMap<Long, TreeSet<Long>> iteratorNeighbors = new TreeMap<>();

    private long numArcs = 0;
    private long numNodes = 0;

    public SimulatedGraph() {}

    private Iterator<Long> emptyLongIterator(){
        return new Iterator<Long>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Long next() {
                return null;
            }
        };
    }

    @Override
    public Object clone() {
        try {
            SimulatedGraph clone = (SimulatedGraph) super.clone();
            clone.iteratorNeighbors = new TreeMap<>();
            for (Map.Entry<Long, TreeSet<Long>> entry : iteratorNeighbors.entrySet()) {
                clone.iteratorNeighbors.put(entry.getKey(), (TreeSet<Long>) entry.getValue().clone());
            }

            return clone;
        } catch(CloneNotSupportedException e) {
            throw new RuntimeException("Simulated graph should never throw this",e);
        }
    }



    @Override
    public IGraph copy(){
        SimulatedGraph copy = new SimulatedGraph();
        copy.iteratorNeighbors = (TreeMap<Long, TreeSet<Long>>) iteratorNeighbors.clone();
        copy.numArcs = numArcs;
        copy.numNodes = numNodes;
        return copy;
    }

    public void addNode(long node) {

        if(!iteratorNeighbors.containsKey(node)) {
            numNodes = Math.max(node+1,numNodes);
        }

    }

    @Override
    public boolean addEdges(Edge ... edges){
        boolean allInserted = true;

        for (int i = 0; i < edges.length; i++) {
            allInserted &= addEdge(edges[i]);
        }

        return allInserted;
    }

    @Override
    public boolean addEdge(Edge edge){

        if(!containsNode(edge.from)) {
            addNode(edge.from);
        }
        if(!containsNode(edge.to)) {
            addNode(edge.to);
        }

        TreeSet<Long> neighbors = iteratorNeighbors.get(edge.from);

        if(neighbors == null){
            neighbors = new TreeSet<>();
            iteratorNeighbors.put(edge.from,neighbors);
        }
        boolean wasAdded = neighbors.add(edge.to);

        if(wasAdded)
            numArcs++;
        return wasAdded;
    }

    public boolean deleteEdge(Edge edge) {
        Set<Long> neighbors = iteratorNeighbors.get(edge.from);
        if(neighbors != null) {
            boolean wasRemoved = neighbors.remove(edge.to);
            if(wasRemoved)
                numArcs--;
            return wasRemoved;
        }else
            return false;
    }

    public Iterator<Long> getLongIterator(long node){

        Set<Long> neighbor = iteratorNeighbors.get(node);
        if(neighbor == null)
            return emptyLongIterator();
        else
            return neighbor.iterator();

    }

    @Override
    public long getNumberOfNodes() {
        return numNodes;
    }

    @Override
    public NodeIterator getNodeIterator(long node) {
        return new SimulatedGraphNodeIterator(node,this);
    }

    @Override
    public NodeIterator getNodeIterator(){
        return getNodeIterator(0);
    }

    @Override
    public long getOutdegree(long node){
        Set<Long> neighbors = iteratorNeighbors.get(node);
        return neighbors == null ? 0 : neighbors.size();
    }

    @Override
    public LazyLongIterator getSuccessors(long node){
        return new SimulatedGraphSuccessorsIterator(getLongIterator(node));
    }

    @Override
    public long getNumberOfArcs(){
        return numArcs;
    }

    @Override
    public IGraph transpose() {
        SimulatedGraph transpose = new SimulatedGraph();
        transpose.addNode(this.numNodes - 1); /* -1 as 0-indexed */
        Edge[] currentEdges = this.getAllEdges();

        for (int i = 0; i < currentEdges.length; i++) {
            Edge currentEdge = currentEdges[i];
            Edge flippedEdge = new Edge(currentEdge.to, currentEdge.from);
            transpose.addEdge(flippedEdge);
        }

        return transpose;
    }

    @Override
    public void store(String outputFile) {
        SimulatedGraphWrapper wrapper = new SimulatedGraphWrapper(this);
        try {
            BVGraph.store(wrapper, outputFile, 0, 0, -1, -1, 0, new ProgressLogger());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long getMemoryUsageBytes() {
        return Utils.getMemoryUsage(this);
    }

    private static class SimulatedGraphNodeIterator extends NodeIterator{

        private long currentIndex;
        private SimulatedGraph graph;
        private long outdegree;

        public SimulatedGraphNodeIterator(long startAt, SimulatedGraph graph){
            currentIndex = startAt-1;
            this.graph = graph;
            outdegree = graph.getOutdegree(startAt);
        }

        @Override
        public long outdegree() {
            return outdegree;
        }

        @Override
        public boolean hasNext() {
            return currentIndex+1 < graph.numNodes;
        }

        @Override
        public long nextLong() {
            if(!hasNext())
                return -1;
            currentIndex++;
            outdegree = graph.getOutdegree(currentIndex);
            return currentIndex;
        }

        @Override
        public LazyLongIterator successors(){
            return graph.getSuccessors(currentIndex);
        }

    }

    private class SimulatedGraphSuccessorsIterator implements LazyLongIterator{

        Iterator<Long> it;

        SimulatedGraphSuccessorsIterator(Iterator<Long> successors){
            it = successors;
        }

        @Override
        public long nextLong() {
            if(!it.hasNext()) {
                return -1;
            }
            return it.next();
        }

        @Override
        public long skip(long l) {
            int i = 0;
            while(it.hasNext() && i++ < l) it.next();
            return i;
        }
    }

    /**
     * Returns an array of all the edges in the graph.
     * Mainly used for testing purposes as it is slow
     * to iterate through all edges, return them and then
     * iterate them again.
     * Works in O(m)
     * @return List of all edges
     */
    public Edge[] getAllEdges() {
        ArrayList<Edge> edges = new ArrayList<>();
        this.iterateAllEdges(edge -> {
            edges.add(edge);
            return null;
        });

        Edge[] edgeArray = new Edge[edges.size()];
        edges.toArray(edgeArray);
        return edgeArray;
    }

}
