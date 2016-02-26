package se.meltwater.vertexcover;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import se.meltwater.graph.Edge;
import se.meltwater.graph.Node;

import java.util.BitSet;
import java.util.HashMap;

/**
 * Created by johan on 2016-02-26.
 */
public class DynamicVertexCover implements IDynamicVertexCover {

    private HashMap<Integer, Integer> maximalMatching = new HashMap<>();
    private BitSet vertexCover = new BitSet();
    private ImmutableGraph graph;

    public  DynamicVertexCover(ImmutableGraph graph) {
        this.graph = graph;
    }

    @Override
    public void insertEdge(Edge edge) {
        if(isInVertexCover(edge.from) || isInVertexCover(edge.to)) {
            return;
        }

        maximalMatching.put(edge.from.id, edge.to.id);

        vertexCover.set(edge.from.id);
        vertexCover.set(edge.to.id);
    }

    @Override
    public void deleteEdge(Edge edge) {
        if(!isInMaximalMatching(edge)) {
            return;
        }

        removeEdgeFromMaximalMatching(edge);
        removeEdgeFromVertexCover(edge);

        checkEndpointOfDeletion(edge.from);
        checkEndpointOfDeletion(edge.to);
    }

    public void checkEndpointOfDeletion(Node node) {
        NodeIterator nodeIterator = graph.nodeIterator(node.id);

        nodeIterator.nextLong();
        LazyLongIterator successors;
        successors = nodeIterator.successors();
        long degree = nodeIterator.outdegree();

        while( degree != 0 ) {
            long successorOfCurrentNode = successors.nextLong();

            if(!isInVertexCover(new Node((int)successorOfCurrentNode))){
                maximalMatching.put(node.id, (int)successorOfCurrentNode);
                vertexCover.set(node.id);
                vertexCover.set((int)successorOfCurrentNode);

                break;
            }

            degree--;
        }
    }

    @Override
    public boolean isInVertexCover(Node node) {
        return vertexCover.get(node.id);
    }

    public boolean isInMaximalMatching(Edge edge) {
        Integer value = maximalMatching.get(edge.from.id);
        if(value == null) {
            return false;
        }

        if(value != edge.to.id) {
            return false;
        }

        return true;
    }

    private void removeEdgeFromMaximalMatching(Edge edge) {
        if(!isInMaximalMatching(edge)) {
            return;
        }

        maximalMatching.remove(edge.from.id);
    }

    private void removeEdgeFromVertexCover(Edge edge) {
        vertexCover.set(edge.from.id, false);
        vertexCover.set(edge.to.id,   false);
    }

    public int getVertexCoverSize() {
        return vertexCover.cardinality();
    }

    public int getMaximalMatchingSize() {
        return maximalMatching.size();
    }
}
