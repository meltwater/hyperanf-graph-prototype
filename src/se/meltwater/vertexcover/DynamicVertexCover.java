package se.meltwater.vertexcover;

import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;

import java.util.BitSet;
import java.util.HashMap;

/**
 * Maintains a 2-approximate Vertex Cover by
 * calculating a maximal matching and for every
 * edge in the matching we pick both nodes to the VC.
 *
 * Needs a reference to the graph to be able to perform deletions.
 *
 * Inspired by the "simple implementation" of Fully dynamic maintenance of vertex cover
 * by Zoran Ivković and Errol L. Lloyd.
 *
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

    /**
     * Deletes an edge and updates the Vertex Cover/Maximal matching
     * @param edge The deleted edge
     */
    @Override
    public void deleteEdge(Edge edge) {
        if(!isInMaximalMatching(edge)) {
            return;
        }

        removeEdgeFromMaximalMatching(edge);
        removeEdgeFromVertexCover(edge);

        checkOutgoingEdgesFromDeletedEndpoint(edge.from);
        checkOutgoingEdgesFromDeletedEndpoint(edge.to);
        checkIncomingEdgesToDeletedEndpoints(edge);
    }

    /**
     * Takes an node that was deleted from the Vertex Cover.
     * Neighbors of this node might now be uncovered, and we must
     * check every outgoing edge to determine if it needs to be added
     * to the maximal matching.
     * @param currentNode A node deleted from the Vertex Cover
     */
    public void checkOutgoingEdgesFromDeletedEndpoint(long currentNode) {
        graph.setNodeIterator(currentNode);
        long degree = graph.getOutdegree();

        while( degree != 0 ) {
            long successorOfCurrentNode = graph.getNextNeighbor();

            if(!isInVertexCover(successorOfCurrentNode)){
                Edge edge = new Edge(currentNode, successorOfCurrentNode);
                addEdgeToMaximalMatching(edge);
                addEdgeToVertexCover(edge);
                break;
            }

            degree--;
        }
    }

    /**
     * Takes an edge deleted from the Maximal Matching.
     * Each endpoint of the edge might have previously covered
     * incoming edges which now are uncovered. To determine what incoming
     * edges are uncovered we loop through all edges and test them.
     * @param edge An edge deleted from the Maximal Matching
     */
    public void checkIncomingEdgesToDeletedEndpoints(Edge edge) {
        for(int currentNode = 0; currentNode < graph.getNumberOfNodes(); currentNode++) {
            if(isInVertexCover((long)currentNode)) {
                continue;
            }

            graph.setNodeIterator(currentNode);
            long degree = graph.getOutdegree();
            while( degree-- != 0 ) {
                long successorOfCurrentNode = graph.getNextNeighbor();

                if(!(successorOfCurrentNode == edge.from) && !(successorOfCurrentNode == edge.to)) {
                    continue;
                }

                if(!isInVertexCover(successorOfCurrentNode)){
                    Edge incomingEdge = new Edge(currentNode, successorOfCurrentNode);
                    addEdgeToMaximalMatching(incomingEdge);
                    addEdgeToVertexCover(incomingEdge);
                    break;
                }
            }
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

    private void addEdgeToMaximalMatching(Edge edge) {
        maximalMatching.put(edge.from, edge.to);
    }

    private void addEdgeToVertexCover(Edge edge) {
        vertexCover.set((int)edge.from);
        vertexCover.set((int)edge.to);
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
