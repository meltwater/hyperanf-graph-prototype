package se.meltwater;

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
    private IGraph graph;
    private int historyRecords;

    public NodeHistory(IDynamicVertexCover vertexCover, int historyRecords, int log2m, IGraph graph){

        vc = vertexCover;
        this.historyRecords = historyRecords;
        history = new HyperLolLolCounterArray[historyRecords];
        for(int i = 0; i < historyRecords; i++)
            history[i] = new HyperLolLolCounterArray(vc.getVertexCoverSize(),vc.getVertexCoverSize(),log2m);
        this.graph = graph;
        int i = 0;
        for(long node : vc.getNodesInVertexCover())
            counterIndex.put(node,(long)(i++));

    }

    public void recalculateHistory(long node) throws InterruptedException {
        if(vc.isInVertexCover(node)) {
            long counterInd = counterIndex.get(node);
            for(HyperLolLolCounterArray counter : history)
                counter.clearCounter(counterInd);
            MSBreadthFirst msbfs = new MSBreadthFirst(new int[]{(int) node}, graph, recalculateVisitor(node,counterInd));
            msbfs.breadthFirstSearch();
            for(int i = 1; i < historyRecords; i++)
                history[i].union(history[i-1]);
        }

    }

    public void calculateHistory(long node, HyperLolLolCounterArray addInto){
        if(vc.isInVertexCover(node)){

        }
    }

    public MSBreadthFirst.Visitor recalculateVisitor(long node, long nodeIndex){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth) -> {
            if(depth > 0){

                if(vc.isInVertexCover(visitNode)){
                    history[depth].union(nodeIndex,history[historyRecords-depth], counterIndex.get(visitNode));
                    bfsVisits.clear();
                }else{
                    history[depth].add(nodeIndex,visitNode);
                }

            }
        };
    }

}
