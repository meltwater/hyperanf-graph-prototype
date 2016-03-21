package se.meltwater;

import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * // TODO class description
 */
public class NodeHistory {
    private IGraph graph;
    private IDynamicVertexCover vc;

    private HashMap<Long,Long> counterIndex;
    private long nextFreeCounterIndex = 0;

    public HyperLolLolCounterArray[] history;

    private int h;

    public NodeHistory(IDynamicVertexCover vertexCover, int h, IGraph graph){

        vc = vertexCover;
        this.h = h;
        history = new HyperLolLolCounterArray[h];
        this.graph = graph;

        counterIndex = new HashMap<>();
        for(long node : vc.getNodesInVertexCover()) {
            insertNodeToCounterIndex(node);
        }
    }

    /**
     * Adds an edge into the NodeHistory. If the nodes of the edge are added to
     * the VC, we allocate memory in the lower history counters and calculate their history.
     * Always allocates memory to the top history counter if any node in the edge is new.
     *
     * @param edge The new edge
     * @throws InterruptedException
     */
    public void addEdge(Edge edge) throws InterruptedException {
        addNewNodes(edge);

        // TODO Must implement addEdge to immutableGraph
        SimulatedGraph sgraph = (SimulatedGraph) graph;
        sgraph.addEdge(edge);

        Map<Long, IDynamicVertexCover.AffectedState> affectedNodes = vc.insertEdge(edge);

        /* As inserting edges can only result in nodes being added
         * to the VC, all affected nodes will be of type AffectedState.Added
         * and will need new memory for all of these */
        allocateMemoryInAllHistoryCounters(affectedNodes.size());

        updateAffectedNodes(affectedNodes);
    }



    /**
     * Adds new nodes in the edge to the graph.
     * This will also allocate memory in the top counterArray
     * as it should always have counters for all nodes.
     * @param edge
     */
    private void addNewNodes(Edge edge) {
        if(!graph.containsNode(edge.from)) { /* Check for readability purpose, not actually necessary */
            addNode(edge.from);
        }
        if(!graph.containsNode(edge.to)) {
            addNode(edge.to);
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

            if (entry.getValue() == IDynamicVertexCover.AffectedState.Added) {
                recalculateHistory(node);
            } else if (entry.getValue() == IDynamicVertexCover.AffectedState.Removed) {
                throw new RuntimeException("Removed nodes not supported in NodeHistory.updateAffectedNodes");
            }

            // TODO When deleteEdge is added there should be a case here
        }
    }

    /**
     * Adds a node to the graph and allocates memory for it in the top
     * history counter (Which should have memory for all nodes)
     * If the node already exists in the graph, nothing is done.
     * @param node
     */
    public void addNode(long node) {
        if(graph.containsNode(node)) {
            return;
        }

        // TODO Must implement addNode to immutableGraph
        SimulatedGraph sgraph = (SimulatedGraph) graph;
        sgraph.addNode(node);

        history[h-1].addCounters(1);
    }

    /**
     * Allocates {@code newCounters} new counters in all history counters.
     * @param newCounters The number of counters to allocate
     */
    private void allocateMemoryInAllHistoryCounters(int newCounters) {
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
        if(h == this.h) {
            history[h-1] = counter;
        } else {
            history[h - 1] = counter.extract(vc.getNodesInVertexCoverIterator(), vc.getVertexCoverSize());
        }
    }

    public double count(long node, int h){
        if(!vc.isInVertexCover(node))
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
     * @param node
     * @throws InterruptedException If the parallell breadth-first search reached time-out
     * @throws Exception
     */
    public void recalculateHistory(long node) throws InterruptedException {
        if(vc.isInVertexCover(node)) {

            for (int i = 0; i < h; i++) {
                HyperLolLolCounterArray counter = history[i];
                long counterInd = getNodeIndex(node, i + 1);
                counter.clearCounter(counterInd);
                counter.add(counterInd,node);
            }

            MSBreadthFirst msbfs = new MSBreadthFirst(new int[]{(int) node}, graph, recalculateVisitor(node));
            msbfs.breadthFirstSearch();
            for(int i = 1; i < h; i++) {
                long counterInd = getNodeIndex(node, i + 1);
                long counterLower = getNodeIndex(node, i );
                history[i].union(counterInd, history[i - 1], counterLower);
            }
        }
    }

    public void calculateHistory(long node, HyperLolLolCounterArray addInto){
        if(vc.isInVertexCover(node)){

        }
    }

    public MSBreadthFirst.Visitor recalculateVisitor(long node){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth) -> {

            if(depth > 0){
                synchronized (this) {
                    if (vc.isInVertexCover(visitNode)) {
                        for (int i = h - 1; i >= depth; i--) {
                            long counterInd = getNodeIndex(node, i + 1);
                            history[i].union(counterInd, history[i - depth], getNodeIndex(visitNode, i - depth));
                        }
                        history[depth - 1].add(getNodeIndex(node, depth), visitNode);
                        bfsVisits.clear();
                    } else {
                        history[depth - 1].add(getNodeIndex(node, depth), visitNode);
                    }
                }
            }
        };
    }
}
