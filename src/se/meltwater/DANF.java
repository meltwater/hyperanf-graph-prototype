package se.meltwater;

import javafx.util.Pair;
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
     * Adds an edge into the DANF. If the nodes of the edge are added to
     * the VC, we allocate memory in the lower history counters and calculate their history.
     * Always allocates memory to the top history counter if any node in the edge is new.
     *
     * @param edge The new edge
     * @throws InterruptedException
     */
    public void addEdge(Edge edge) throws InterruptedException {
        addNewNodes(edge);
        Map<Long, IDynamicVertexCover.AffectedState> affectedNodes = vc.insertEdge(edge);

        graph.addEdge(edge);

        /* As inserting edges can only result in nodes being added
         * to the VC, all affected nodes will be of type AffectedState.Added
         * and will need new memory for all of these */
        allocateMemoryInBottomHistoryCounters(affectedNodes.size());

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


        for(Map.Entry<Long, IDynamicVertexCover.AffectedState> entry : affectedNodes.entrySet()) {
            long node = entry.getKey();

            if (entry.getValue() == IDynamicVertexCover.AffectedState.Added) {
                recalculateHistory(node);
            } else if (entry.getValue() == IDynamicVertexCover.AffectedState.Removed) {
                // TODO When deleteEdge is added there should be a case here
                throw new RuntimeException("Removed nodes not supported in DANF.updateAffectedNodes");
            }
        }
    }

    /**
     * Adds a node to the graph and allocates memory for it in the top
     * history counter (Which should have memory for all nodes)
     * If the node already exists in the graph, nothing is done.
     * @param node
     */
    public void addNodeToTopLevel(long node) {
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
     * @param node
     * @throws InterruptedException If the parallel breadth-first search reached time-out
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

            ArrayList<Pair<BitSet,Long>> calculations = new ArrayList<>();

            int[] sources = new int[]{(int) node};
            MSBreadthFirst msbfs = new MSBreadthFirst(sources, graph, recalculateVisitor(node, getNodeIndex(node,0), calculations));
            BitSet[] seen = msbfs.breadthFirstSearch();
            int[] sourceIndices = new int[sources.length];
            for (int i = 0; i < sources.length; i++) {
                sourceIndices[i] = (int)getNodeIndex(sources[i],0);
            }

            for (int i = 1; i < h; i++) {
                int[] trueSourceIndices = i+1 == h ? sources : sourceIndices;
                if(i >= 2) {
                    for (int visitNode = 0; visitNode < seen.length; visitNode++) {
                        if (seen[visitNode].cardinality() > 0 && vc.isInVertexCover(visitNode)) {
                            int bfs = -1;
                            long visitIndex = getNodeIndex(visitNode, i - 1);
                            while ((bfs = seen[visitNode].nextSetBit(bfs+1)) != -1) {
                                history[i].union(trueSourceIndices[bfs], history[i - 2], visitIndex);
                            }
                        }
                    }
                }
                if(i >= 1) {
                    for (Pair<BitSet, Long> calc : calculations) {
                        int bfs = -1;
                        long visitIndex = getNodeIndex(calc.getValue(), i);
                        while ((bfs = calc.getKey().nextSetBit(bfs + 1)) != -1) {
                            history[i].union(trueSourceIndices[bfs], history[i - 1], visitIndex);
                        }
                    }
                }
            }

    /*
    Recalculate R ⊆ VC
Global i=1
Do BFS from all v ∊ R
   At depth d for node u with visits b:
       if(i==1 && d==1)
           Hv(1)∪{u}
           if(u ∊ VC)
               copyHistory.add(b.clone(),u,1)
               Break BFS
           synchronize()
       else if(d==2)
           copyHistory(v,u,2)

copyHistory(v,u,distance)
    while(i ≤ h)
        Hv(i) ∪ Hu(i-distance) ∪ Hv(i-1)
        synchronize()

synchronize()
    Wait for all threads to finish
    i = i+1
     */
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
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth) -> {

        };
    }

    private MSBreadthFirst.Visitor recalculateVisitor(long node, long counterInd, ArrayList<Pair<BitSet,Long>> calculations){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth) -> {

            if(depth == 1){
                synchronized (counterIndex.get(node)) {
                    history[0].add(counterInd,visitNode);
                    if(vc.isInVertexCover(visitNode)) {
                        calculations.add(new Pair<>((BitSet) bfsVisits.clone(), visitNode));
                        bfsVisits.clear();
                    }
                }
            }else if(depth == 2) {
                synchronized (counterIndex.get(node)) {
                    history[1].add(h == 2 ? node : counterInd, visitNode);
                    bfsVisits.clear();
                }
            }
        };
    }

}
