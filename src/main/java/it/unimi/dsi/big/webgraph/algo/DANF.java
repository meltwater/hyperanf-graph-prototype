package it.unimi.dsi.big.webgraph.algo;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.big.webgraph.Utils;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.HyperLogLogCounterArray;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * A dynamic approximate neighborhood function calculator. The algorithm
 * is based on HyperANF from the article "HyperANF: Approximating the Neighbourhood
 * Function of Very Large Graphs on a Budget". See {@link HyperBall} which is an extension to
 * HyperANF. DANF is developed in the paper "DANF: Approximate Neighborhood Function on Large
 * Dynamic Graphs".
 *
 * To use this class, start by initiating the class using one of the constructors. A
 * ProgressLogger and the size of partitions can be set by {@link DANF#withPartitionSize(int)} and
 * {@link DANF#withProgressLogger(ProgressLogger)}. When new edges are produced, call
 * {@link DANF#addEdges(Edge...)}. DANF will insert the edges in the graph and the transpose.
 * DANF will also insert the edges into the vertex cover. The graphs and vertex cover
 * specified to DANF should never be modified outside of the instance as that will
 * introduce bugs. The history will be updated and the neighborhood function can be
 * calculated by {@link DANF#count(long, int)}.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class DANF implements DynamicNeighborhoodFunction{

    protected MutableGraph graph;
    protected MutableGraph graphTranspose;
    protected IDynamicVertexCover vc;

    protected long[][] counterIndex;
    protected long nextFreeCounterIndex = 0;

    protected HyperLogLogCounterArray[] history;
    private int counterLongWords;

    protected MSBreadthFirst transposeMSBFS;

    protected int h;

    private boolean closed = false;

    private static final int STATIC_LOGLOG = 0;

    protected ProgressLogger pl = null;
    protected int partitionSize = DEFAULT_PARTITION_SIZE;

    public static final int DEFAULT_PARTITION_SIZE = 5000;

    /**
     * Creates the graph transpose. Initiates the vertex cover. Runs
     * HyperBall and saves the history.
     *
     * @param h     The number of hops of the neighborhood function that should be calculated.
     * @param log2m The number of register bits that should be used (the logarithm of the number of registers).
     * @param graph The graph which the neighborhood function should be calculated on.
     */
    public DANF(int h, int log2m, MutableGraph graph){
        this(h,log2m,graph,Util.randomSeed(),new DynamicVertexCover(graph));
    }

    /**
     *
     * Creates the graph transpose. Initiates the vertex cover. Runs
     * HyperBall using the specified seed and saves the history.
     *
     * @param h     The number of hops of the neighborhood function that should be calculated.
     * @param log2m The number of register bits that should be used (the logarithm of the number of registers).
     * @param graph The graph which the neighborhood function should be calculated on.
     * @param seed  The seed to use for HyperBall
     */
    public DANF(int h, int log2m, MutableGraph graph, long seed){
        this(h,log2m,graph,seed,new DynamicVertexCover(graph));
    }

    /**
     *
     * Creates the graph transpose. Runs HyperBall using the specified seed and saves the history.
     *
     * @param h     The number of hops of the neighborhood function that should be calculated.
     * @param log2m The number of register bits that should be used (the logarithm of the number of registers).
     * @param graph The graph which the neighborhood function should be calculated on.
     * @param seed  The seed to use for HyperBall
     * @param vc    The vertex cover to use
     */
    public DANF(int h, int log2m, MutableGraph graph, long seed, IDynamicVertexCover vc){
        this(h,log2m,graph,graph.transpose(),seed,vc);
    }

    /**
     *
     * Initiates the vertex cover. Runs HyperBall and saves the history.
     *
     * @param h     The number of hops of the neighborhood function that should be calculated.
     * @param log2m The number of register bits that should be used (the logarithm of the number of registers).
     * @param graph The graph which the neighborhood function should be calculated on.
     * @param graphTranspose The graph transpose
     */
    public DANF(int h, int log2m, MutableGraph graph, MutableGraph graphTranspose){
        this(h,log2m,graph,graphTranspose,Util.randomSeed(),new DynamicVertexCover(graph));
    }


    /**
     *
     * Runs HyperBall using the specified seed and saves the history.
     *
     * @param h     The number of hops of the neighborhood function that should be calculated.
     * @param log2m The number of register bits that should be used (the logarithm of the number of registers).
     * @param graph The graph which the neighborhood function should be calculated on.
     * @param graphTranspose The graph transpose
     * @param seed The seed to use for HyperBall
     * @param vertexCover The vertex cover to use
     */
    public DANF(int h, int log2m, MutableGraph graph, MutableGraph graphTranspose, long seed,
                IDynamicVertexCover vertexCover){

        vc = vertexCover;
        this.h = h;
        history = new HyperLogLogCounterArray[h];
        this.graph = graph;
        this.graphTranspose = graphTranspose;

        transposeMSBFS = new MSBreadthFirst(graphTranspose);

        counterIndex = LongBigArrays.newBigArray(vc.getVertexCoverSize());

        LazyLongIterator vcIterator = vc.getNodesInVertexCoverIterator();
        long vcSize = vc.getVertexCoverSize();
        long node;
        while(vcSize-- > 0) {
            node = vcIterator.nextLong();
            insertNodeToCounterIndex(node);
        }

        if(pl != null)
            pl.logger().info("Starting HyperBall calculation.");
        HyperBall hyperBall = new HyperBall(graph,graphTranspose,log2m,seed,pl);
        hyperBall.init();
        try {
            for (int i = 1; i <= h; i++) {
                hyperBall.iterate();
                addHistory(hyperBall.getCounter(), i);
            }
        }catch (IOException e){
            throw new RuntimeException("Something went wrong with the temporary graph files", e);
        }finally {
            try {
                hyperBall.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Set the partition size that will be used on inserted edges. The bigger the partition,
     * the faster the algorithm runs, but the more memory is required.
     *
     * @param partitionSize
     * @return
     */
    public DANF withPartitionSize(int partitionSize){
        this.partitionSize = partitionSize;
        return this;
    }

    /**
     * Use a progress logger to monitor the progress of inserted edges.
     *
     * @param pl
     * @return
     */
    public DANF withProgressLogger(ProgressLogger pl){
        this.pl = pl;
        return this;
    }

    /**
     * Shuts down the MSBreadthFirst threads
     */
    @Override
    public void close(){
        if(!closed) {
            transposeMSBFS.close();
            closed = true;
        }
    }

    public MutableGraph getGraph() {
        return graph;
    }

    public int getMaxH(){
        return h;
    }

    public IDynamicVertexCover getDynamicVertexCover() {
        return vc;
    }

    public HyperLogLogCounterArray getCounter(int h){
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
    @Override
    public void addEdges(Edge ... edges)  {
        Map<Long, IDynamicVertexCover.AffectedState> affectedNodes = new HashMap<>();

        Edge[] flippedEdges = new Edge[edges.length];
        long maxNode = 0;
        int i;
        for (i = 0; i < edges.length; i++) {
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
            long previousHighestNode = graph.numNodes()-1;
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

            if (entry.getValue() == IDynamicVertexCover.AffectedState.ADDED) {

                calculateIncompleteHistory(node);

            } else if (entry.getValue() == IDynamicVertexCover.AffectedState.REMOVED) {
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

        LazyLongIterator successors = graph.successors(node);
        long degree = graph.outdegree(node);

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
    private void addHistory(HyperLogLogCounterArray counter, int h){
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
     * The neighborhood function with reach less than the specified h
     * in the constructor is only available for nodes in the vertex cover.
     *
     * @param node
     * @param h
     * @return The approximate neighborhood function for the specified node using the specified number of hops.
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
        int i = 0;
        for(HyperLogLogCounterArray counter : history) {
            ret[i] = counter.count(getNodeIndex(node, i + 1));
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
            history[STATIC_LOGLOG].add(node, historyBits[0]);
            for (int i = 1; i < h; i++) {
                history[i-1].getCounter(getNodeIndex(node, i), historyBits[i]);
            }
        } else {
            LazyLongIterator successors = graph.successors(node);
            long degree = graph.outdegree(node);

            for (int i = 0; i < h; i++) {
                history[STATIC_LOGLOG].add(node, historyBits[i]);
            }

            while(degree-- > 0 ) {
                long neighbor = successors.nextLong();

                if(h > 1)
                    history[STATIC_LOGLOG].add(neighbor, historyBits[1]);
                for (int i = 1; i < h-1; i++) {
                    history[STATIC_LOGLOG].add(neighbor, historyBits[i + 1]);

                    long[] neighborBits = new long[counterLongWords];
                    history[i-1].getCounter(getNodeIndex(neighbor, i), neighborBits);
                    history[i].max(historyBits[i + 1], neighborBits);
                }
            }
        }
        return historyBits;
    }

    /**
     * Propagates the effects of the added edges
     *
     * @param edges
     */
    private void propagate(Edge ... edges) {
        sortEdgesByDANFValues(edges);

        /* otherSourceNodes are used to prune BFSs early. If a BFS reach a source node of a later
         * BFS, the current can be pruned. */
        LongOpenHashSet otherSourceNodes = getSourceNodesAsHashSet(edges);

        try {
            initiateLogger(edges);
            PropagationTraveler[] travelers = new PropagationTraveler[Math.min(partitionSize, edges.length)];
            long[] fromNodes = new long[Math.min(partitionSize, edges.length)];
            for (int i = 0, j = 0; i < edges.length; i++, j++) {

                travelers[j] = generateTraveler(edges[i]);
                fromNodes[j] = edges[i].from;

                /* Remove nodes from the current partition as BFSs should not prune at sources of the current partition. */
                otherSourceNodes.remove(edges[i].from);

                if (j == partitionSize - 1) {
                    transposeMSBFS.breadthFirstSearch(fromNodes, propagateVisitor(otherSourceNodes), travelers);
                    if(pl != null)
                        pl.update();

                    if (edges.length - i - 1 < partitionSize) {
                        travelers = new PropagationTraveler[edges.length - i - 1];
                        fromNodes = new long[edges.length - i - 1];
                    }
                    j = -1;
                }
            }

            if (fromNodes.length > 0) {
                transposeMSBFS.breadthFirstSearch(fromNodes, propagateVisitor(otherSourceNodes), travelers);
                if(pl != null)
                    pl.update();
            }
            if(pl != null)
                pl.stop();

        }catch (InterruptedException e){
            throw new RuntimeException("An error occurred when performing the breadth first search",e);
        }
    }

    private void initiateLogger(Edge[] edges) {
        if(pl != null) {
            pl.itemsName = "Partitions";
            pl.expectedUpdates = (int) Math.ceil((float) edges.length / partitionSize);
            pl.start("Starting insertion of " + edges.length + " edges in " +
                    (int)Math.ceil((float) edges.length / partitionSize) + " partitions of size " + partitionSize);
        }
    }

    /**
     * Returns a new Traveler using the nodes present in {@code edge}.
     * @param edge
     * @return
     */
    private PropagationTraveler generateTraveler(Edge edge) {
         return new PropagationTraveler(getTravelerHistory(edge));
    }

    /**
     * Returns the history that should follow the traveler
     * from the to node of {@code edge}.
     * @param edge
     * @return
     */
    private long[][] getTravelerHistory(Edge edge) {
        long[][] travelerHistory = calculateHistory(edge.to);

        if(vc.isInVertexCover(edge.from)) {
            addNodesHistoryToTravelerHistory(edge.from, travelerHistory);
        }
        return travelerHistory;
    }

    /**
     * Unions the history of the edge's to node with {@code travelerHistory}.
     * Only level 1 to h will be merged.
     *
     * Used to add the from node's history to the traveler history before propagating.
     * @param node
     * @param travelerHistory
     */
    private void addNodesHistoryToTravelerHistory(long node, long[][] travelerHistory) {
        long[] visitorHistory = new long[counterLongWords];

        for (int k = 0; k < h; k++) {
            long visitNodeIndex = getNodeIndex(node, k + 1);
            history[k].getCounter(visitNodeIndex, visitorHistory);
            history[k].max(travelerHistory[k], visitorHistory);
        }
    }

    /**
     * This method is used to speed up the MS-BFS.
     * Sorting the partitions by DANF values increase
     * the performance of the MS-BFS by up to
     * 40%.
     *
     * The edges will only be sorted if they will be partitioned.
     *
     * The edges will be sorted in the existing array.
     *
     * @param edges The edges to sort
     */
    private void sortEdgesByDANFValues(Edge[] edges) {
        if(edges.length > partitionSize * 1.1) {
            double[] values = new double[(int)graph.numNodes()];
            for (Edge e : edges) {
                values[(int) e.from] = count(e.from, h);
            }

            Arrays.sort(edges, (e1, e2) -> Double.compare(values[(int)e1.from], values[(int)e2.from]));
        }
    }

    /**
     * Returns a hash set of all the source nodes present
     * in an edge in {@code edges}. This will only be performed
     * if the edges will be partitioned.
     *
     * @param edges The edges
     * @return A hash set of source nodes or an empty hash set
     *         if the edges shouldn't be partitioned.
     */
    private LongOpenHashSet getSourceNodesAsHashSet(Edge[] edges) {
        LongOpenHashSet sourceNodes = new LongOpenHashSet();

        if(edges.length > partitionSize * 1.1) {
            for (Edge edge : edges) {
                sourceNodes.add(edge.from);
            }
        }

        return sourceNodes;
    }


    public long getMemoryUsageGraphBytes() {
        return graph.getMemoryUsageBytes() + graphTranspose.getMemoryUsageBytes();
    }

    public long getMemoryUsageCounterBytes() {
        return Utils.getMemoryUsage(counterIndex, history);
    }

    public long getMemoryUsageVCBytes() {
        return vc.getMemoryUsageBytes();
    }

    public long getMemoryUsageMsBfsBytes() {
        return transposeMSBFS.getMemoryUsageBytes(trav -> (long)((PropagationTraveler)trav).bits.length*counterLongWords*Long.BYTES);
    }

    @Override
    public long getMemoryUsageBytes() {
        return graph.getMemoryUsageBytes() + graphTranspose.getMemoryUsageBytes() +
                Utils.getMemoryUsage(vc, counterIndex, history) +
                transposeMSBFS.getMemoryUsageBytes(trav -> (long)((PropagationTraveler)trav).bits.length*counterLongWords*Long.BYTES);
    }

    private MSBreadthFirst.Visitor propagateVisitor(LongOpenHashSet otherSourceNodes){
        boolean needsSync = !history[STATIC_LOGLOG].longwordAligned;

        return (long visitNode, BitSet bfsVisits, BitSet seen, int d, MSBreadthFirst.Traveler t) -> {
            int depth = d + 1;
            PropagationTraveler propTraver = (PropagationTraveler) t;

            if (vc.isInVertexCover(visitNode)) {
                long[] visitNodeBits = new long[counterLongWords];
                for (int i = 0; i < h + 1 - depth; i++) {
                    long visitNodeIndex = getNodeIndex(visitNode, i + depth);
                    int historyIndex = i + depth - 1;
                    unionVisitNodeWithTraveler(needsSync, propTraver, visitNodeBits, visitNodeIndex, historyIndex, i);
                }
            } else {
                long[] visitNodeBits = new long[counterLongWords];
                unionVisitNodeWithTraveler(needsSync, propTraver, visitNodeBits, visitNode, h - 1, h-depth);
            }

            if (depth == h || otherSourceNodes.contains(visitNode)) {
                bfsVisits.clear();
            }
        };
    }

    private void unionVisitNodeWithTraveler(boolean needsSync, PropagationTraveler propTraver, long[] visitNodeBits, long visitNodeIndex, int historyIndex, int bitsIndex) {
        history[historyIndex].getCounter(visitNodeIndex, visitNodeBits);
        history[historyIndex].max(visitNodeBits, propTraver.bits[bitsIndex]);

        if(needsSync) {
            synchronized (history[historyIndex]) {
                history[historyIndex].setCounter(visitNodeBits, visitNodeIndex);
            }
        }else
            history[historyIndex].setCounter(visitNodeBits, visitNodeIndex);
    }

    private class PropagationTraveler extends MSBreadthFirst.Traveler{
        long[][] bits;

        PropagationTraveler(long[][] bits){
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
                history[STATIC_LOGLOG].max(clonedBits[i], otherTraveler.bits[i]);
            }
            return shouldClone() ? new PropagationTraveler(clonedBits) : this;
        }
    }
}
