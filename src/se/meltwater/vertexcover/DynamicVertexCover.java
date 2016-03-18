package se.meltwater.vertexcover;

import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;

import java.util.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
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

    public DynamicVertexCover(IGraph graph) {
        this.graph = graph;

        graph.iterateAllEdges(edge -> {
            insertEdge(edge);
            return null;
        });
    }

    @Override
    public Map<Long, AffectedState> insertEdge(Edge edge) {
        Map<Long, AffectedState> affectedNodes = new HashMap<>();
        if(isInVertexCover(edge.from) || isInVertexCover(edge.to)) {
            return affectedNodes;
        }

        addEdgeToMaximalMatching(edge);
        addEdgeToVertexCover(edge);

        updateAffectedNodesFromEdge(edge, AffectedState.Added, affectedNodes);

        return affectedNodes;
    }

    /**
     * Deletes an edge and updates the Vertex Cover/Maximal matching
     * @param edge The deleted edge
     */
    @Override
    public Map<Long, AffectedState> deleteEdge(Edge edge) {
        Set<Long> removedNodes = new HashSet<>();
        Set<Long> addedNodes   = new HashSet<>();

        Map<Long, AffectedState> affectedNodes = new HashMap<>();
        if(!isInMaximalMatching(edge)) {
            return affectedNodes;
        }

        removeEdgeFromMaximalMatching(edge);
        removeEdgeFromVertexCover(edge);

        removedNodes.add(edge.from);
        removedNodes.add(edge.to);

        checkOutgoingEdgesFromDeletedEndpoint(edge.from, addedNodes);
        checkOutgoingEdgesFromDeletedEndpoint(edge.to, addedNodes);
        checkIncomingEdgesToDeletedEndpoints(edge, addedNodes);

        affectedNodes = createAffectedNodes(removedNodes, addedNodes);

        return affectedNodes;
    }

    private Map<Long,AffectedState> createAffectedNodes(Set<Long> removedNodes, Set<Long> addedNodes) {
        Map<Long, AffectedState> affectedNodes = new HashMap<>();
        for(Long removedNode : removedNodes) {
            updateAffectedNodes(removedNode, AffectedState.Removed, affectedNodes);
        }
        for(Long addedNode : addedNodes) {
            updateAffectedNodes(addedNode, AffectedState.Added, affectedNodes);
        }

        return affectedNodes;
    }

    /**
     * Inserts both endpoints of the edge with the specified state.
     * @param edge
     * @param state
     * @param affectedNodes
     */
    public void updateAffectedNodesFromEdge(Edge edge, AffectedState state, Map<Long, AffectedState> affectedNodes) {
        updateAffectedNodes(edge.from, state, affectedNodes);
        updateAffectedNodes(edge.to,   state, affectedNodes);
    }

    /**
     * If a node gets marked as both Added and Removed it is removed from {@code affectedNodes},
     * else it is inserted with the specified state.
     * @param node Node to insert to affected nodes
     * @param state Enum saying how the node was affected
     * @param affectedNodes Currently affected nodes
     */
    public static void updateAffectedNodes(Long node, AffectedState state, Map<Long, AffectedState> affectedNodes) {
        AffectedState mappedState = affectedNodes.get(node);

        if(mappedState == state) {
            return;
        }

        if(mappedState == null) {
            affectedNodes.put(node, state);
        } else { //We have the node both as added and removed = not affected
            affectedNodes.remove(node);
        }
    }

    /**
     * Takes an node that was deleted from the Vertex Cover.
     * Neighbors of this node might now be uncovered, and we must
     * check every outgoing edge to determine if it needs to be added
     * to the maximal matching.
     * @param currentNode A node deleted from the Vertex Cover
     */
    public void checkOutgoingEdgesFromDeletedEndpoint(long currentNode, Set<Long> addedNodes) {
        graph.setNodeIterator(currentNode);
        long degree = graph.getOutdegree();

        while( degree != 0 ) {
            long successorOfCurrentNode = graph.getNextNeighbor();

            if(!isInVertexCover(successorOfCurrentNode)){
                Edge edge = new Edge(currentNode, successorOfCurrentNode);
                addEdgeToMaximalMatching(edge);
                addEdgeToVertexCover(edge);

                addedNodes.add(edge.from);
                addedNodes.add(edge.to);

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
    public void checkIncomingEdgesToDeletedEndpoints(Edge edge, Set<Long> addedNodes) {
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

                    addedNodes.add(Long.valueOf(currentNode));
                    addedNodes.add(Long.valueOf(successorOfCurrentNode));

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

    @Override
    public long[] getNodesInVertexCover(){
        long[] ret = new long[vertexCover.cardinality()];
        int node = -1, i = 0;
        while ((node = vertexCover.nextSetBit(node+1)) != -1)
            ret[i++] = node;
        return ret;
    }

    @Override
    public int getVertexCoverSize() {
        return vertexCover.cardinality();
    }

    public int getMaximalMatchingSize() {
        return maximalMatching.size();
    }
}
