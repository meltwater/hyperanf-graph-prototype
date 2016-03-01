package se.meltwater.vertexcover;

import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;

import java.util.BitSet;
import java.util.HashMap;

/**
 * Created by johan on 2016-02-26.
 */
public class DynamicVertexCover implements IDynamicVertexCover {

    private HashMap<Long, Long> maximalMatching = new HashMap<>();
    private BitSet vertexCover = new BitSet();
    private IGraph graph;

    public  DynamicVertexCover(IGraph graph) {
        this.graph = graph;
    }

    @Override
    public void insertEdge(Edge edge) {
        if(isInVertexCover(edge.from) || isInVertexCover(edge.to)) {
            return;
        }

        maximalMatching.put(edge.from, edge.to);

        vertexCover.set((int)edge.from);
        vertexCover.set((int)edge.to);
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
        checkEndpointsOfDeletion(edge);
    }

    public void checkEndpointsOfDeletion(Edge edge) {
        for(int i = 0; i < graph.getNumberOfNodes(); i++) {
            if(isInVertexCover((long)i)) {
                continue;
            }

            graph.setNodeIterator(i);
            long degree = graph.getOutdegree();
            while( degree != 0 ) {
                long successorOfCurrentNode = graph.getNextNeighbor();

                if(successorOfCurrentNode == edge.from || successorOfCurrentNode == edge.to) {

                    if(!isInVertexCover(successorOfCurrentNode)){
                        maximalMatching.put((long)i, successorOfCurrentNode);
                        vertexCover.set(i);
                        vertexCover.set((int)successorOfCurrentNode);

                        break;
                    }
                }

                degree--;
            }
        }
    }


    public void checkEndpointOfDeletion(long node) {
        graph.setNodeIterator(node);
        long degree = graph.getOutdegree();

        while( degree != 0 ) {
            long successorOfCurrentNode = graph.getNextNeighbor();

            if(!isInVertexCover(successorOfCurrentNode)){
                maximalMatching.put(node, successorOfCurrentNode);
                vertexCover.set((int)node);
                vertexCover.set((int)successorOfCurrentNode);

                break;
            }

            degree--;
        }
    }

    @Override
    public boolean isInVertexCover(long node) {
        return vertexCover.get((int)node);
    }

    public boolean isInMaximalMatching(Edge edge) {
        Long value = maximalMatching.get(edge.from);
        if(value == null) {
            return false;
        }

        if(value != edge.to) {
            return false;
        }

        return true;
    }

    private void removeEdgeFromMaximalMatching(Edge edge) {
        if(!isInMaximalMatching(edge)) {
            return;
        }

        maximalMatching.remove(edge.from);
    }

    private void removeEdgeFromVertexCover(Edge edge) {
        vertexCover.set((int)edge.from, false);
        vertexCover.set((int)edge.to,   false);
    }

    public int getVertexCoverSize() {
        return vertexCover.cardinality();
    }

    public int getMaximalMatchingSize() {
        return maximalMatching.size();
    }
}
