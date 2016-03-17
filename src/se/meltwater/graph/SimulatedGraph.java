package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import sun.reflect.generics.tree.Tree;

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
public class SimulatedGraph extends IGraph implements  Cloneable {


    private long nodeIterator = 0;
    private Iterator<Long> successors;
    private TreeMap<Long, HashSet<Long>> iteratorNeighbors = new TreeMap<>();
    private long numArcs = 0;

    @Override
    public Object clone() throws CloneNotSupportedException {
        SimulatedGraph clone = (SimulatedGraph) super.clone();
        clone.iteratorNeighbors = new TreeMap<>();
        for(Map.Entry<Long, HashSet<Long>> entry : iteratorNeighbors.entrySet()) {
            clone.iteratorNeighbors.put(entry.getKey(), (HashSet<Long>) entry.getValue().clone());
        }

        return clone;
    }


    public void addNode(long node) {
        iteratorNeighbors.put(node, new HashSet<>());
    }

    public void addEdge(Edge edge){
        HashSet neighbors = iteratorNeighbors.get(edge.from);
        neighbors.add(edge.to);
        numArcs++;
    }

    public void deleteEdge(Edge edge) {
        HashSet neighbors = iteratorNeighbors.get(edge.from);
        neighbors.remove(edge.to);
        numArcs--;
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

    @Override
    public long getNumberOfArcs() { return numArcs; }

    @Override
    public NodeIterator getNodeIterator(long node) {
        return new SimulatedGraphNodeIterator(node,this);
    }

    public NodeIterator getNodeIterator(){
        return new SimulatedGraphNodeIterator(iteratorNeighbors.firstKey(),this);
    }

    private static class SimulatedGraphNodeIterator extends NodeIterator{

        private long currentIndex;
        private SimulatedGraph graph;
        private long outdegree;

        public SimulatedGraphNodeIterator(long startAt, SimulatedGraph graph){
            currentIndex = startAt-1;
            this.graph = graph;
            outdegree = graph.iteratorNeighbors.get(startAt).size();
        }

        @Override
        public long outdegree() {
            return outdegree;
        }

        @Override
        public boolean hasNext() {
            return currentIndex < graph.iteratorNeighbors.lastKey();
        }

        @Override
        public long nextLong() {
            if(!hasNext())
                throw new IllegalStateException("No more nodes");
            currentIndex = graph.iteratorNeighbors.higherKey(currentIndex);
            outdegree = graph.iteratorNeighbors.get(currentIndex).size();
            return currentIndex;
        }

        @Override
        public long[][] successorBigArray() {
            HashSet<Long> succs = graph.iteratorNeighbors.get(currentIndex);
            long[][] arr = LongBigArrays.newBigArray(succs.size());
            int i = 0;
            for(long succ : succs)
                LongBigArrays.add(arr,i++,succ);
            return arr;
        }

    }

}
