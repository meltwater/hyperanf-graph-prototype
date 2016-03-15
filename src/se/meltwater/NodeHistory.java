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

    }

    public void recalculateHistory(long node){
        if(vc.isInVertexCover(node)) {
            long counterInd = counterIndex.get(node);
            for(HyperLolLolCounterArray counter : history)
                counter.clearCounter(counterInd);
            MSBreadthFirst msbfs = new MSBreadthFirst(new int[]{(int) node}, graph, recalculateVisitor(node,counterInd));
        }

    }

    public MSBreadthFirst.Visitor recalculateVisitor(long node, long nodeIndex){
        return (long visitNode, BitSet bfsVisits, BitSet seen, int depth) -> {
            if(depth > 0){

                if(vc.isInVertexCover(visitNode)){
                    //history[historyRecords-depth-1];
                }

            }
        };
    }

}
