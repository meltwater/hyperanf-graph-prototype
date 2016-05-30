package se.meltwater.algo;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.bfs.MSBreadthFirst;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.utils.Utils;
import se.meltwater.vertexcover.DynamicVertexCover;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class DANF implements DynamicNeighborhoodFunction{

    private IGraph graph;
    private IGraph graphTranspose;
    private IDynamicVertexCover vc;

    private long[][] counterIndex;
    private long nextFreeCounterIndex = 0;

    private HyperLolLolCounterArray[] history;
    private int counterLongWords;

    private MSBreadthFirst transposeMSBFS;

    private int h;

    private long cachedNode = -1, cachedNodeIndex;
    private boolean closed = false;

    private final int STATIC_LOLOL = 0;

    private int found = 0;
    private int visited = 0;


    public DANF(int h, int log2m, IGraph graph){
        this(new DynamicVertexCover(graph),h,log2m,graph,Util.randomSeed());
    }

    public DANF(int h, int log2m, IGraph graph, long seed){
        this(new DynamicVertexCover(graph),h,log2m,graph,seed);
    }

    public DANF(IDynamicVertexCover vertexCover, int h, int log2m, IGraph graph, long seed){

        vc = vertexCover;
        this.h = h;
        history = new HyperLolLolCounterArray[h];
        this.graph = graph;
        this.graphTranspose = graph.transpose();

        transposeMSBFS = new MSBreadthFirst(graphTranspose);

        counterIndex = LongBigArrays.newBigArray(vc.getVertexCoverSize());

        LazyLongIterator vcIterator = vc.getNodesInVertexCoverIterator();
        long vcSize = vc.getVertexCoverSize(), node;
        while(vcSize-- > 0) {
            node = vcIterator.nextLong();
            insertNodeToCounterIndex(node);
        }

        HyperBoll hyperBoll = new HyperBoll(graph,graphTranspose,log2m,seed);
        hyperBoll.init();
        try {
            for (int i = 1; i <= h; i++) {
                hyperBoll.iterate();
                addHistory(hyperBoll.getCounter(), i);
            }
        }catch (IOException e){
            throw new RuntimeException("Something went wrong with the temporary graph files", e);
        }finally {
            try {
                hyperBoll.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Shuts down the MSBreadthFirst threads
     */
    public void close(){
        if(!closed) {
            transposeMSBFS.close();
            closed = true;
        }
    }

    public IGraph getGraph() {
        return graph;
    }

    public int getMaxH(){
        return h;
    }

    public IDynamicVertexCover getDynamicVertexCover() {
        return vc;
    }

    public HyperLolLolCounterArray getCounter(int h){
        checkH(h);
        return history[h-1];
    }

    /**
     *
     * Adds the specified {@code edges} to the graph and recalculates
     * the neighborhood functions.
     *
     * @param edges
     * @throws InterruptedException
     */
    public void addEdges(Edge ... edges)  {
        Map<Long, IDynamicVertexCover.AffectedState> affectedNodes = new HashMap<>();

        Edge[] flippedEdges = new Edge[edges.length];
        long maxNode = 0;
        for (int i = 0; i < edges.length; i++) {
            Edge edge = edges[i];
            maxNode = Math.max(maxNode,Math.max(edge.to,edge.from));
            affectedNodes.putAll(vc.insertEdge(edge));
            flippedEdges[i] = edge.flip();
        }
        addNodeToTopLevel(maxNode);

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
     * Updates every node that have been affected by a change in the graph.
     * For insertions it sets a mapping index to the lower histories and
     * recalculates their history.
     * Deletions are NOT supported yet
     * @param affectedNodes
     */
    private void updateAffectedNodes(Map<Long, IDynamicVertexCover.AffectedState> affectedNodes)  {
        for(Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : affectedNodes.entrySet()) {
            long node = entry.getKey();
            insertNodeToCounterIndex(node);
        }

        for(Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : affectedNodes.entrySet()) {
            long node = entry.getKey();

            if (entry.getValue() == IDynamicVertexCover.AffectedState.Added) {

                calculateIncompleteHistory(node);

            } else if (entry.getValue() == IDynamicVertexCover.AffectedState.Removed) {
                // TODO When deleteEdge is added there should be a case here
                throw new RuntimeException("Removed nodes not supported in DANF.updateAffectedNodes");
            }
        }
    }

    /**
     *
     * When a node has been added to the vertex cover, calculate its history
     * based on the surrounding nodes.
     *
     * If there was an edge to a neighbor previously
     * that node have to be in the vertex cover as otherwise that edge would not
     * have been covered.
     *
     * If there were no edge to a neighbor previously, that node may not be in
     * the vertex cover and may not have a history. However, as the edge was added
     * in this bulk the neighbor will propagate its history to this node so the history
     * will be correct after the propagation.
     *
     * @param node The node just added to the VC
     */
    private void calculateIncompleteHistory(long node){

        LazyLongIterator successors = graph.getSuccessors(node);
        long degree = graph.getOutdegree(node);

        for (int i = 0; i < h; i++) {
            history[i].add(getNodeIndex(node,i+1),node);
        }

        while(degree-- > 0 ) {
            long neighbor = successors.nextLong();

            if(!vc.isInVertexCover(neighbor))
                continue;

            history[0].add(getNodeIndex(node,1),neighbor);
            for (int i = 1; i < h; i++) {
                history[i].add(getNodeIndex(node,i+1),neighbor);

                history[i].union(getNodeIndex(node,i+1), history[i-1],getNodeIndex(neighbor,i));
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

    private void checkH(int h){
        if(h > this.h || h <= 0)
            throw new IllegalArgumentException("The given h (" + h + ") must be more than 0 and less than or equal to max h (" + this.h + ").");
    }

    private void checkNode(long node){
        if(!graph.containsNode(node))
            throw new IllegalArgumentException("The given node (" + node + ") doesn't exist in the graph.");
    }

    /**
     * Returns the index of {@code node} in the HyperLolLol counters
     * @param node
     * @return
     */
    private long getNodeIndex(long node, int h){

        checkH(h);
        if (h == this.h) {
            return node;
        }


        return LongBigArrays.get(counterIndex,node);
    }

    /**
     * Assigns {@code node} to a new unique index in the HyperLolLol counters
     * @param node
     */
    private void insertNodeToCounterIndex(long node) {
        counterIndex = LongBigArrays.grow(counterIndex,node+1);
        LongBigArrays.set(counterIndex,node,nextFreeCounterIndex++);
    }

    /**
     * Sets the history of a specific level
     * @param counter
     * @param h The level to set
     */
    private void addHistory(HyperLolLolCounterArray counter, int h){
        checkH(h);
        counterLongWords = counter.counterLongwords;
        if(h == this.h) {
            history[h-1] = counter;
        } else {
            history[h - 1] = counter.extract(vc.getNodesInVertexCoverIterator(), vc.getVertexCoverSize());
        }
    }

    /**
     *
     * Returns the approximate neighborhood function for the given
     * node with reach {@code h}.
     *
     * @param node
     * @param h
     * @return
     */
    public double count(long node, int h) {
        checkH(h);
        checkNode(node);
        if(!vc.isInVertexCover(node) && h != this.h)
            throw new IllegalArgumentException("Node " + node + " wasn't in the vertex cover.");
        return history[h-1].count(getNodeIndex(node, h));
    }

    public double[] count(long node){
        checkNode(node);
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
     * Fetches the registry history of the specified node.
     * If the node is not in the vertex cover; the history is calculated
     * from the surrounding vertex cover nodes.
     *
     * @param node
     * @return
     */
    private long[][] calculateHistory(long node){
        long[][] historyBits = new long[h][counterLongWords];
        if(vc.isInVertexCover(node)) {
            history[STATIC_LOLOL].add(node, historyBits[0]);
            for (int i = 1; i < h; i++) {
                history[i-1].getCounter(getNodeIndex(node, i), historyBits[i]);
            }
        } else {
            LazyLongIterator successors = graph.getSuccessors(node);
            long degree = graph.getOutdegree(node);

            for (int i = 0; i < h; i++) {
                history[STATIC_LOLOL].add(node, historyBits[i]);
            }

            while(degree-- > 0 ) {
                long neighbor = successors.nextLong();

                if(h > 1)
                    history[STATIC_LOLOL].add(neighbor, historyBits[1]);
                for (int i = 1; i < h-1; i++) {
                    history[STATIC_LOLOL].add(neighbor, historyBits[i + 1]);

                    long[] neighborBits = new long[counterLongWords];
                    history[i-1].getCounter(getNodeIndex(neighbor, i), neighborBits);
                    history[i].max(historyBits[i + 1], neighborBits);
                }
            }
        }
        return historyBits;
    }

    /**
     *
     * Propagates the effects of the added edges
     *
     * @param edges
     * @throws InterruptedException
     */
    private void propagate(Edge ... edges) {
        int partitionSize = 5000;

        LongOpenHashSet otherSourceNodes = new LongOpenHashSet();
        if(edges.length > partitionSize * 1.1) {

            double[] values = new double[(int)graph.getNumberOfNodes()];
            for (int i = 0; i < edges.length; i++) {
                Edge e = edges[i];
                //values[(int) e.from] = graph.getOutdegree(e.from);
                values[(int) e.from] = count(e.from, h);
            }

            Arrays.sort(edges, (e1, e2) -> Double.compare(values[(int)e1.from], values[(int)e2.from]));

            for (int i = 0; i < edges.length; i++) {
                otherSourceNodes.add(edges[i].from);
            }
        }


        try {

            PropagationTraveler[] travelers = new PropagationTraveler[Math.min(partitionSize, edges.length)];
            long[] fromNodes = new long[Math.min(partitionSize, edges.length)];
            for (int i = 0, j = 0; i < edges.length; i++, j++) {

                long[][] travelerHistory = calculateHistory(edges[i].to);
                long[] visitorHistory = new long[counterLongWords];

                if(vc.isInVertexCover(edges[i].from)) {
                    for (int k = 0; k < h; k++) {
                        long visitNodeIndex = getNodeIndex(edges[i].from, k + 1);
                        history[k].getCounter(visitNodeIndex, visitorHistory);
                        history[k].max(travelerHistory[k], visitorHistory);
                    }
                }

                travelers[j] = new PropagationTraveler(travelerHistory);
                fromNodes[j] = edges[i].from;
                otherSourceNodes.remove(edges[i].from);
                if (j == partitionSize - 1) {
                    transposeMSBFS.breadthFirstSearch(fromNodes, propagateVisitor(otherSourceNodes), travelers);
                    System.out.println("Pruned paths: " + found + " ;Visited : " + visited);

                    if (edges.length - i - 1 < partitionSize) {
                        travelers = new PropagationTraveler[edges.length - i - 1];
                        fromNodes = new long[edges.length - i - 1];
                    }
                    j = -1;
                    System.out.println("Completed: " + i + " nodes.");
                }
            }

            if (fromNodes.length > 0) {
                transposeMSBFS.breadthFirstSearch(fromNodes, propagateVisitor(otherSourceNodes), travelers);
                System.out.println("Pruned paths: " + found + " ;Visited : " + visited);
            }

        }catch (InterruptedException e){
            throw new RuntimeException("An error occurred when performing the breadth first search",e);
        }
    }


    public long getMemoryUsageGraphBytes() {
        return graph.getMemoryUsageBytes() + graphTranspose.getMemoryUsageBytes();
    }

    public long getMemoryUsageCounterBytes() {
        return Utils.getMemoryUsage(counterIndex) + Utils.getMemoryUsage(history);
    }

    public long getMemoryUsageVCBytes() {
        return vc.getMemoryUsageBytes();
    }

    public long getMemoryUsageMsBfsBytes() {
        return transposeMSBFS.getMemoryUsageBytes(trav -> (long)((PropagationTraveler)trav).bits.length*counterLongWords*Long.BYTES);
    }

    public long getMemoryUsageBytes() {

        long msbfsBytes = transposeMSBFS.getMemoryUsageBytes(trav -> (long)((PropagationTraveler)trav).bits.length*counterLongWords*Long.BYTES);
        long otherBytes = Utils.getMemoryUsage(vc, counterIndex, history) + msbfsBytes;

        System.out.println("Ratio: " + msbfsBytes / (float)otherBytes);

        System.out.println("Graph: " + graph.getMemoryUsageBytes() + ". transpose: " + graphTranspose.getMemoryUsageBytes() +
                ". vc: " + Utils.getMemoryUsage(vc) + ". counterIndex: " + Utils.getMemoryUsage(counterIndex) + ". history: " + Utils.getMemoryUsage(history) + ". " +
                "MSBFS: " + msbfsBytes +
                ". Ratio: " + (float)msbfsBytes/(Utils.getMemoryUsage(vc, counterIndex, history) + msbfsBytes) +
                ". Ratio history:" + (float)Utils.getMemoryUsage(history)/((Utils.getMemoryUsage(vc, counterIndex, history) + msbfsBytes)));

        System.out.println("VC Size: " + vc.getVertexCoverSize() + ", Total nodes: " + graph.getNumberOfNodes() + ", ratio: " + vc.getVertexCoverSize() / (float)graph.getNumberOfNodes() );

        return graph.getMemoryUsageBytes() + graphTranspose.getMemoryUsageBytes() +
                Utils.getMemoryUsage(vc, counterIndex, history) +
                transposeMSBFS.getMemoryUsageBytes(trav -> (long)((PropagationTraveler)trav).bits.length*counterLongWords*Long.BYTES);
    }

    private MSBreadthFirst.Visitor propagateVisitor(LongOpenHashSet otherSourceNodes){
        boolean needsSync = true;//!history[STATIC_LOLOL].longwordAligned;
        return (long visitNode, BitSet bfsVisits, BitSet seen, int d, MSBreadthFirst.Traveler t) -> {
            visited++;
            int depth = d + 1;

            PropagationTraveler propTraver = (PropagationTraveler) t;

            if (vc.isInVertexCover(visitNode)) {
                long[] visitNodeBits = new long[counterLongWords];
                long visitNodeIndex;
                int index;
                for (int i = 0; i < h + 1 - depth; i++) {
                    visitNodeIndex = getNodeIndex(visitNode, i + depth);
                    index = i + depth - 1;
                    history[index].getCounter(visitNodeIndex, visitNodeBits);

                    history[index].max(visitNodeBits, propTraver.bits[i]);

                    if(needsSync) {
                        synchronized (history[index]) {
                            history[index].setCounter(visitNodeBits, visitNodeIndex);
                        }
                    }else
                        history[i + depth - 1].setCounter(visitNodeBits, visitNodeIndex);
                }
            } else {//if(!otherSourceNodes.contains(visitNode)){
                long[] visitNodeBits = new long[counterLongWords];
                history[h - 1].getCounter(visitNode, visitNodeBits);

                history[h - 1].max(visitNodeBits, propTraver.bits[h-depth]);

                if(needsSync) {
                    synchronized (history[h - 1]) {
                        history[h - 1].setCounter(visitNodeBits, visitNode);
                    }
                }else
                    history[h - 1].setCounter(visitNodeBits, visitNode);
            }


            if (depth == h || otherSourceNodes.contains(visitNode)) {
                if(otherSourceNodes.contains(visitNode)) {
                    found++;
                }
                bfsVisits.clear();
            }
        };
    }

    private class PropagationTraveler extends MSBreadthFirst.Traveler{
        public long[][] bits;

        public PropagationTraveler(long[][] bits){
            this.bits = bits;
        }

        @Override
        public MSBreadthFirst.Traveler merge(MSBreadthFirst.Traveler mergeWith, int d) {

            int depth = d + 1;
            long[][] clonedBits = shouldClone() ? new long[h + 1 - depth][counterLongWords] : bits;
            PropagationTraveler otherTraveler = (PropagationTraveler) mergeWith;

            for (int i = 0; i < clonedBits.length; i++) {
                if(shouldClone())
                    clonedBits[i] = bits[i].clone();
                history[STATIC_LOLOL].max(clonedBits[i], otherTraveler.bits[i]);
            }
            return shouldClone() ? new PropagationTraveler(clonedBits) : this;
        }
    }
}
