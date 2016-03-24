package se.meltwater.algo;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import javafx.util.Pair;
import se.meltwater.MSBreadthFirst;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * // TODO class description
 */
public class DANF {
    private IGraph graph;
    private IDynamicVertexCover vc;

    private HashMap<Long,Long> counterIndex;
    private long nextFreeCounterIndex = 0;

    private HyperLolLolCounterArray[] history;

    private int h;

    public DANF(IDynamicVertexCover vertexCover, int h, IGraph graph){

        vc = vertexCover;
        this.h = h;
        history = new HyperLolLolCounterArray[h];
        this.graph = graph;

        counterIndex = new HashMap<>();
        for(long node : vc.getNodesInVertexCover()) {
            insertNodeToCounterIndex(node);
        }
    }


    public void addEdges(Edge ... edges) throws InterruptedException {
        Map<Long, IDynamicVertexCover.AffectedState> affectedNodes = new HashMap<>();
        for(Edge edge : edges) {
            addNewNodes(edge);
            affectedNodes.putAll(vc.insertEdge(edge));
        }

        graph.addEdges(edges);

        /* As inserting edges can only result in nodes being added
         * to the VC, all affected nodes will be of type AffectedState.Added
         * and will need new memory for all of these */
        allocateMemoryInBottomHistoryCounters(affectedNodes.size());

        updateAffectedNodes(affectedNodes); //TODO FIX THEZ
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
            long previousHighestNode = graph.getNumberOfNodes();
            long nodesToAdd = node - previousHighestNode + 1;
            history[h-1].addCounters(nodesToAdd);
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
        IntArrayList inVC = new IntArrayList();
        for (long node : nodes) {
            if(vc.isInVertexCover(node))
                inVC.add((int)node);
            else{
                LazyLongIterator succs = graph.getSuccessors(node);
                long out = graph.getOutdegree(node);
                for (int neighborI = 0; neighborI < out ; neighborI++) {
                    long neighbor = succs.nextLong();
                    history[h-1].union(node,history[h-2],getNodeIndex(neighbor,h-1));
                }
            }
        }
        if(inVC.size() == 0)
            return;

        int[] nodesInVc = inVC.toIntArray();
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
            for (int node : nodesInVc) {
                long counterInd = getNodeIndex(node, i + 1);
                long counterLower = getNodeIndex(node, i);
                history[i].union(counterInd, history[i - 1], counterLower);
            }
        }

    }

    public void calculateHistory(long node, HyperLolLolCounterArray addInto){
        if(vc.isInVertexCover(node)){

        }
    }

    private void propagate(long node){

        /*
        if reg(u,index(v)) > hash(v) //Our added node (v) isn’t max
             if u is in vertex cover
                set H = registry history of u
             else
                set H_0 = this node
                set H_x = union of H_{x-1} from all incoming neighbors
             check which level i in H that reg(u,index(v)) first appears.
             if(i <= l) // reg(u,index(v)) reaches further than hash(v)
                prune this DFS

          if u is in vertex cover
             forall k: h>k>=l; H_k.reg(u,index(v)) := hash(v)
          else
             reg(u,index(v)) := hash(v)

        reg(u,i) = register of u at index i
         */
    }

    private MSBreadthFirst.Visitor propagateVisitor(){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth, MSBreadthFirst.Traveler t) -> {

        };
    }

    private MSBreadthFirst.Visitor recalculateVisitor(int[] bfsSources){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth, MSBreadthFirst.Traveler t) -> {

            if(depth > 0){
                synchronized (this) {
                    if (vc.isInVertexCover(visitNode)) {
                        for (int i = h - 1; i >= depth; i--) {
                            long visitIndex = getNodeIndex(visitNode, i - depth + 1);
                            int bfs = -1;
                            while((bfs = bfsVisits.nextSetBit(bfs+1)) != -1)
                                history[i].union(getNodeIndex(bfsSources[bfs],i+1), history[i - depth], visitIndex);
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

}
