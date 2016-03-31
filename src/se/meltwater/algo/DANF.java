package se.meltwater.algo;

import it.unimi.dsi.big.webgraph.LazyLongIterator;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashBigSet;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import se.meltwater.MSBreadthFirst;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.vertexcover.IDynamicVertexCover;


import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 *
 *
 */
public class DANF {
    private IGraph graph;
    private IGraph graphTranspose;
    private IDynamicVertexCover vc;

    private HashMap<Long,Long> counterIndex;
    private long nextFreeCounterIndex = 0;

    private HyperLolLolCounterArray[] history;
    private int counterLongWords;

    private int h;

    private final int STATIC_LOLOL = 0;

    public DANF(IDynamicVertexCover vertexCover, int h, IGraph graph){

        vc = vertexCover;
        this.h = h;
        history = new HyperLolLolCounterArray[h];
        this.graph = graph;
        this.graphTranspose = graph.transpose();

        counterIndex = new HashMap<>();
        for(long node : vc.getNodesInVertexCover()) {
            insertNodeToCounterIndex(node);
        }
    }


    public void addEdges(Edge ... edges) throws InterruptedException {
        Map<Long, IDynamicVertexCover.AffectedState> affectedNodes = new HashMap<>();

        Edge[] flippedEdges = new Edge[edges.length];
        for(int i = 0; i < edges.length; i++) {
            Edge edge = edges[i];
            addNewNodes(edge);
            affectedNodes.putAll(vc.insertEdge(edge));
            flippedEdges[i] = edge.flip();
        }

        graph.addEdges(edges);
        graphTranspose.addEdges(flippedEdges);

        /* As inserting edges can only result in nodes being added
         * to the VC, all affected nodes will be of type AffectedState.Added
         * and will need new memory for all of these */
        allocateMemoryInBottomHistoryCounters(affectedNodes.size());

        updateAffectedNodes(affectedNodes);

        propagate(edges);
    }

    /**
     * Adds new nodes in the edge to the graph.
     * This will also allocate memory in the top counterArray
     * as it should always have counters for all nodes.
     * @param edge
     */
    private void addNewNodes(Edge edge) {
        if(!graph.containsNode(edge.from)) { /* Check for readability purpose, not actually necessary */
            addNodeToTopLevel(edge.from);
        }
        if(!graph.containsNode(edge.to)) {
            addNodeToTopLevel(edge.to);
        }
    }

    /**
     * Updates every node that have been affected by a change in the graph.
     * For insertions it sets a mapping index to the lower histories and
     * recalculates their history.
     * Deletions are NOT supported yet
     * @param affectedNodes
     * @throws InterruptedException
     */
    private void updateAffectedNodes(Map<Long, IDynamicVertexCover.AffectedState> affectedNodes) throws InterruptedException {
        for(Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : affectedNodes.entrySet()) {
            long node = entry.getKey();
            insertNodeToCounterIndex(node);
        }

        LongArrayList addedNodes = new LongArrayList();
        for(Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : affectedNodes.entrySet()) {
            long node = entry.getKey();

            if (entry.getValue() == IDynamicVertexCover.AffectedState.Added) {
                addedNodes.add(node);
            } else if (entry.getValue() == IDynamicVertexCover.AffectedState.Removed) {
                // TODO When deleteEdge is added there should be a case here
                throw new RuntimeException("Removed nodes not supported in DANF.updateAffectedNodes");
            }
        }
        recalculateHistory(addedNodes.toLongArray());
    }

    /**
     * Adds a node to the graph and allocates memory for it in the top
     * history counter (Which should have memory for all nodes)
     * If the node already exists in the graph, nothing is done.
     * @param node
     */
    private void addNodeToTopLevel(long node) {
        if(!graph.containsNode(node)) {
            long previousHighestNode = graph.getNumberOfNodes()-1;
            long nodesToAdd = node - previousHighestNode;
            history[h-1].addCounters(nodesToAdd);
            for (long n = previousHighestNode+1; n <= node ; n++) {
                history[h-1].add(n,n);
            }
        }
    }

    /**
     * Allocates {@code newCounters} new counters in all history counters.
     * @param newCounters The number of counters to allocate
     */
    private void allocateMemoryInBottomHistoryCounters(int newCounters) {
        for (int i = 0; i < history.length - 1; i++) {
            history[i].addCounters(newCounters );
        }
    }

    /**
     * Returns the index of {@code node} in the HyperLolLol counters
     * @param node
     * @return
     */
    private long getNodeIndex(long node, int h){
        if (h == this.h) {
            return node;
        }
        return counterIndex.get(node);
    }

    /**
     * Assigns {@code node} to a new unique index in the HyperLolLol counters
     * @param node
     */
    private void insertNodeToCounterIndex(long node) {
        counterIndex.put(node, nextFreeCounterIndex++);
    }

    /**
     * Sets the history of a specific level
     * @param counter
     * @param h The level to set
     */
    public void addHistory(HyperLolLolCounterArray counter, int h){
        counterLongWords = counter.counterLongwords;
        if(h == this.h) {
            history[h-1] = counter;
        } else {
            history[h - 1] = counter.extract(vc.getNodesInVertexCoverIterator(), vc.getVertexCoverSize());
        }
    }

    public double count(long node, int h){
        if(!vc.isInVertexCover(node) && h != this.h)
            throw new IllegalArgumentException("Node " + node + " wasn't in the vertex cover.");
        return history[h-1].count(getNodeIndex(node, h));
    }

    public double[] count(long node){
        if(!vc.isInVertexCover(node))
            throw new IllegalArgumentException("Node " + node + " wasn't in the vertex cover.");
        double[] ret = new double[h];
        int i=0;
        for(HyperLolLolCounterArray counter : history) {
            ret[i] = counter.count(getNodeIndex(node, i+1));
            i++;
        }
        return ret;

    }

    /**
     *
     * @param nodes
     * @throws InterruptedException If the parallel breadth-first search reached time-out
     */
    public void recalculateHistory(long ... nodes) throws InterruptedException {
        LongArrayList inVC = new LongArrayList();
        for (long node : nodes) {
            if(vc.isInVertexCover(node))
                inVC.add(node);
            else{
                LazyLongIterator succs = graph.getSuccessors(node);
                long out = graph.getOutdegree(node);
                history[h-1].add(node,node);
                for (long neighborI = 0; neighborI < out ; neighborI++) {
                    long neighbor = succs.nextLong();
                    if(h > 1)
                        history[h-1].union(node,history[h-2],getNodeIndex(neighbor,h-1));
                    else
                        history[h-1].add(node,neighbor);
                }
            }
        }
        if(inVC.size() == 0)
            return;

        long[] nodesInVc = inVC.toLongArray();
        for (int i = 0; i < h; i++) {
            HyperLolLolCounterArray counter = history[i];
            for(long node : nodesInVc) {
                long counterInd = getNodeIndex(node, i + 1);
                counter.clearCounter(counterInd);
                counter.add(counterInd, node);
            }
        }

        MSBreadthFirst msbfs = new MSBreadthFirst(nodesInVc, graph, recalculateVisitor(nodesInVc));
        msbfs.breadthFirstSearch();

        for(int i = 1; i < h; i++) {
            for (long node : nodesInVc) {
                long counterInd = getNodeIndex(node, i + 1);
                long counterLower = getNodeIndex(node, i);
                history[i].union(counterInd, history[i - 1], counterLower);
            }
        }

    }

    private MSBreadthFirst.Visitor recalculateVisitor(long[] bfsSources){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth, MSBreadthFirst.Traveler t) -> {

            if(depth > 0 && depth <= h){
                synchronized (this) {
                    if (vc.isInVertexCover(visitNode)) {
                        if(h > 1) {
                            for (int i = h - 1; i >= depth; i--) {
                                long visitIndex = getNodeIndex(visitNode, i - depth + 1);
                                int bfs = -1;
                                while ((bfs = bfsVisits.nextSetBit(bfs + 1)) != -1)
                                    history[i].union(getNodeIndex(bfsSources[bfs], i + 1), history[i - depth], visitIndex);
                            }
                        }

                        int bfs = -1;
                        while((bfs = bfsVisits.nextSetBit(bfs+1)) != -1)
                            history[depth - 1].add(getNodeIndex(bfsSources[bfs], depth), visitNode);
                        bfsVisits.clear();
                    } else {
                        int bfs = -1;
                        while((bfs = bfsVisits.nextSetBit(bfs+1)) != -1)
                            history[depth - 1].add(getNodeIndex(bfsSources[bfs], depth), visitNode);
                    }
                }
            }
        };
    }

    public long[][] calculateHistory(long node){
        long[][] historyBits = new long[h + 1][counterLongWords];
        if(vc.isInVertexCover(node)) {
            history[STATIC_LOLOL].add(node, historyBits[0]);
            for (int i = 1; i < h + 1; i++) {
                history[i-1].getCounter(getNodeIndex(node, i), historyBits[i]);
            }
        } else {
            LazyLongIterator successors = graph.getSuccessors(node);
            long degree = graph.getOutdegree(node);

            for (int i = 0; i < h + 1; i++) {
                history[STATIC_LOLOL].add(node, historyBits[i]);
            }

            while(degree-- > 0 ) {
                long neighbor = successors.nextLong();

                history[STATIC_LOLOL].add(neighbor, historyBits[1]);
                for (int i = 1; i < h; i++) {
                    history[STATIC_LOLOL].add(neighbor, historyBits[i + 1]);

                    long[] neighborBits = new long[counterLongWords];
                    history[i-1].getCounter(getNodeIndex(neighbor, i), neighborBits);
                    history[i].max(historyBits[i + 1], neighborBits);
                }
            }
        }
        return historyBits;
    }

    private void propagate(Edge ... edges) throws InterruptedException {

        int partition = 5000;
        PropagationTraveler[] travelers = new PropagationTraveler[Math.min(partition,edges.length)];
        long[] fromNodes = new long[Math.min(partition,edges.length)];
        for (int i = 0,j = 0; i < edges.length ; i++,j++) {
            travelers[j] = new PropagationTraveler(calculateHistory(edges[i].to));
            fromNodes[j] = edges[i].from;
            if(j == partition-1) {
                MSBreadthFirst msbfs = new MSBreadthFirst(fromNodes, travelers, graphTranspose, propagateVisitor());
                msbfs.breadthFirstSearch();
                travelers = new PropagationTraveler[Math.min(partition,edges.length-i-1)];
                fromNodes = new long[Math.min(partition,edges.length-i-1)];
                j = -1;
            }
        }

        if(fromNodes.length > 0) {
            MSBreadthFirst msbfs = new MSBreadthFirst(fromNodes, travelers, graphTranspose, propagateVisitor());
            msbfs.breadthFirstSearch();
        }

    }

    private MSBreadthFirst.Visitor propagateVisitor(){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int d, MSBreadthFirst.Traveler t) -> {

            int depth = d+1;

            PropagationTraveler propTraver = (PropagationTraveler) t;

            if(vc.isInVertexCover(visitNode)) {
                long[] visitNodeBits = new long[counterLongWords];
                long visitNodeHistoryIndex = getNodeIndex(visitNode,0);
                long visitNodeIndex;
                for (int i = 0; i < h+1-depth ; i++) {
                    visitNodeIndex = i+depth-1 == h-1 ? visitNode : visitNodeHistoryIndex;
                    history[i+depth-1].getCounter(visitNodeIndex,visitNodeBits);
                    history[i+depth-1].max(visitNodeBits,propTraver.bits[i]);
                    history[i+depth-1].setCounter(visitNodeBits,visitNodeIndex);
                }
            } else {
                long[] visitNodeBits = new long[counterLongWords];
                history[h-1].getCounter(visitNode, visitNodeBits);
                history[h-1].max(visitNodeBits, propTraver.bits[h-depth]);
                history[h-1].setCounter(visitNodeBits, visitNode);
            }


            if(depth == h) {
                bfsVisits.clear();
            }
        };
    }

    private class PropagationTraveler implements MSBreadthFirst.Traveler{
        public long[][] bits;

        public PropagationTraveler(long[][] bits){
            this.bits = bits;
        }

        @Override
        public MSBreadthFirst.Traveler merge(MSBreadthFirst.Traveler mergeWith, int d) {
            int depth = d+1;
            long[][] clonedBits = new long[h + 1 - depth][counterLongWords];
            PropagationTraveler otherTraveler = (PropagationTraveler) mergeWith;

            for (int i = 0; i < clonedBits.length; i++) {
                clonedBits[i] = bits[i].clone();
                history[STATIC_LOLOL].max(clonedBits[i], otherTraveler.bits[i]);
            }

            return new PropagationTraveler(clonedBits);
        }
    }

}
