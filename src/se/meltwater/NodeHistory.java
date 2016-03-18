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
    private int historyRecords;

    public NodeHistory(IDynamicVertexCover vertexCover, int h, IGraph graph){

        vc = vertexCover;
        this.historyRecords = h-1;
        this.h = h;
        history = new HyperLolLolCounterArray[historyRecords];
        this.graph = graph;

        counterIndex = new HashMap<>();
        for(long node : vc.getNodesInVertexCover()) {
            insertNodeToCounterIndex(node);
        }
    }

    /**
     * Adds an edge into the NodeHistory. If the nodes of the edge are added to
     * the VC, we allocate memory in the history counters and calculate their history.
     *
     * @param edge The new edge
     * @throws InterruptedException
     */
    public void addEdge(Edge edge) throws InterruptedException {
        // TODO Must implement addEdge to immutableGraph
        SimulatedGraph sgraph = (SimulatedGraph) graph;
        sgraph.addEdge(edge);

        Map<Long, IDynamicVertexCover.AffectedState> affectedNodes = vc.insertEdge(edge);

        /* As inserting edges can only result in nodes being added
         * to the VC, all affected nodes will be of type AffectedState.Added
         * and will need new memory for all of these */
        allocateMemoryInAllHistoryCounters(affectedNodes.size());

        for(Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : affectedNodes.entrySet()) {
            long node = entry.getKey();
            insertNodeToCounterIndex(node);

            if(entry.getValue() == IDynamicVertexCover.AffectedState.Added) {
                recalculateHistory(node);
            }
        }
    }

    /**
     * Allocates {@code newCounters} new counters in all history counters.
     * @param newCounters The number of counters to allocate
     */
    private void allocateMemoryInAllHistoryCounters(int newCounters) {
        for (int i = 0; i < history.length; i++) {
            history[i].addCounters(newCounters);
        }
    }

    /**
     * Returns the index of {@code node} in the HyperLolLol counters
     * @param node
     * @return
     */
    private long getNodeIndex(long node){
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
        history[h-1] = counter.extract(vc.getNodesInVertexCoverIterator(),vc.getVertexCoverSize());
    }

    public double count(long node, int h){
        if(!vc.isInVertexCover(node))
            throw new IllegalArgumentException("Node " + node + " wasn't in the vertex cover.");
        return history[h-1].count(getNodeIndex(node));
    }

    public double[] count(long node){
        if(!vc.isInVertexCover(node))
            throw new IllegalArgumentException("Node " + node + " wasn't in the vertex cover.");
        double[] ret = new double[historyRecords];
        int i=0;
        for(HyperLolLolCounterArray counter : history)
            ret[i++] = counter.count(getNodeIndex(node));
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
            long counterInd = getNodeIndex(node);
            for(HyperLolLolCounterArray counter : history) {
                counter.clearCounter(counterInd);
                counter.add(counterInd,node);
            }
            MSBreadthFirst msbfs = new MSBreadthFirst(new int[]{(int) node}, graph, recalculateVisitor(node,counterInd));
            msbfs.breadthFirstSearch();
            for(int i = 1; i < historyRecords; i++)
                history[i].union(counterInd,history[i-1],counterInd);
        }

    }

    public void calculateHistory(long node, HyperLolLolCounterArray addInto){
        if(vc.isInVertexCover(node)){

        }
    }

    public MSBreadthFirst.Visitor recalculateVisitor(long node, long nodeIndex){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth) -> {

            if(depth > 0){
                synchronized (this) {
                    if (vc.isInVertexCover(visitNode)) {
                        for (int i = historyRecords - 1; i >= depth; i--)
                            history[i].union(nodeIndex, history[i - depth], getNodeIndex(visitNode));
                        history[depth - 1].add(nodeIndex, visitNode);
                        bfsVisits.clear();
                    } else {
                        history[depth - 1].add(nodeIndex, visitNode);
                    }
                }

            }
        };
    }

}
