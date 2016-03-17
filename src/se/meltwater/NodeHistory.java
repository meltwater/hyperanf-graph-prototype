package se.meltwater;

import org.apache.commons.lang.ObjectUtils;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.util.BitSet;
import java.util.HashMap;

/**
 * Created by simon on 2016-03-04.
 */
public class NodeHistory {

    private IDynamicVertexCover vc;
    private HashMap<Long,Long> counterIndex;
    private HyperLolLolCounterArray[] history;
    private int h;
    private IGraph graph;
    private int historyRecords;
    private long currentlyUsedMapIndex = 0;

    public NodeHistory(IDynamicVertexCover vertexCover, int h, IGraph graph){

        vc = vertexCover;
        this.historyRecords = h-1;
        this.h = h;
        history = new HyperLolLolCounterArray[historyRecords];
        this.graph = graph;

        counterIndex = new HashMap<>();
        for(long node : vc.getNodesInVertexCover()) {
            counterIndex.put(node, currentlyUsedMapIndex);
            currentlyUsedMapIndex++;
        }
    }



    private long getNodeIndex(long node){
        return node;
    }

    public void addHistory(HyperLolLolCounterArray counter, int h){
        history[h-1] = (HyperLolLolCounterArray) counter.clone();
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
                history[i].union(node,history[i-1],node);
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
