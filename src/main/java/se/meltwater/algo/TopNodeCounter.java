package se.meltwater.algo;

import javafx.util.Pair;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.util.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 *  Provides a continuously updated TreeSet, sorted by the nodes' HLL value. Useful for maintaining
 *  the top nodes in DANF.
 */
public class TopNodeCounter {

    private DANF danf;
    private TreeSet<Pair<Double, Long>> nodesSortedByValue;
    private HashMap<Long, Double> updatedNodesWithValue;
    private long timeOfLastUpdate;
    private static final long UPDATE_INTERVAL_MS = 1000;
    private static final double CHANGE_THREASHOLD_PERCENTAGE = 0.1;

    public TopNodeCounter(DANF danf) {
        this.danf = danf;
        initNodeSets(danf.getGraph());
    }

    private void initNodeSets(IGraph graph) {
        nodesSortedByValue = new TreeSet<Pair<Double, Long>>(nodeScoreComparator());
        updatedNodesWithValue = new HashMap<>();
        timeOfLastUpdate = System.currentTimeMillis();
        for (long node = 0; node < graph.getNumberOfNodes(); node++) {
            nodesSortedByValue.add(new Pair<>(danf.count(node, danf.getMaxH()), node));
        }
    }

    public Map<Long, Double> updateNodeSets(Edge ... edges) {
        insertUpdatedValues(edges);
        long currentTime = System.currentTimeMillis();
        Map<Long, Double> nodesWithPercentageChange = new HashMap<>();
        if(currentTime - timeOfLastUpdate > UPDATE_INTERVAL_MS) {
            mergeNodeSets(nodesWithPercentageChange);
            timeOfLastUpdate = currentTime;
        }

        return nodesWithPercentageChange;
    }

    private void mergeNodeSets(Map<Long, Double> nodesWithPercentageChange) {
        Iterator<Pair<Double, Long>> it = nodesSortedByValue.iterator();
        while(it.hasNext()) {
            Pair<Double, Long> pair = it.next();
            Double currentValue = updatedNodesWithValue.get(pair.getValue());
            if(currentValue != null) { //If null, then the value haven't changed
                double previousValue = pair.getKey();
                double valueChange = currentValue / previousValue;
                if(valueChange > CHANGE_THREASHOLD_PERCENTAGE) {
                    nodesWithPercentageChange.put(pair.getValue(), valueChange);
                }

                it.remove();
            }
        }

        updatedNodesWithValue.forEach((key, value) -> nodesSortedByValue.add(new Pair<>(value, key)));
        updatedNodesWithValue.clear();
    }

    public TreeSet<Pair<Double, Long>> getNodesSortedByValue() {
        return nodesSortedByValue;
    }

    private void insertUpdatedValues(Edge ... edges) {
        for (int i = 0; i < edges.length; i++) {
            Edge edge = edges[i];
            updatedNodesWithValue.put(edge.from, danf.count(edge.from, danf.getMaxH()));
            updatedNodesWithValue.put(edge.to,   danf.count(edge.to,   danf.getMaxH()));
        }
    }

    private Comparator<Pair<Double,Long>> nodeScoreComparator(){
        return (o1,o2) -> {
            int ret = o2.getKey().compareTo(o1.getKey());
            return ret == 0 ? o2.getValue().compareTo(o1.getValue()) : ret;
        };
    }
}
