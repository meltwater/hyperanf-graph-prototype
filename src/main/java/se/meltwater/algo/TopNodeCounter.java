package se.meltwater.algo;

import javafx.util.Pair;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.vertexcover.IDynamicVertexCover;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

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
    private static final long UPDATE_INTERVAL_MS = 1000 * 10;
    private static final double CHANGE_THREASHOLD_PERCENTAGE = 1.5;
    private static final double MIN_NODE_COUNT = 10;
    private Consumer<Set<Pair<Double, Long>>> rapidChangeCallback;

    public TopNodeCounter(DANF danf) {
        this.danf = danf;
        initNodeSets(danf.getGraph());
        rapidChangeCallback = null;
    }

    public void setRapidChangeCallback( Consumer<Set<Pair<Double, Long>>> rapidChangeCallback) {
        this.rapidChangeCallback = rapidChangeCallback;
    }

    private void initNodeSets(IGraph graph) {
        nodesSortedByValue = new TreeSet<Pair<Double, Long>>(nodeScoreComparator());
        updatedNodesWithValue = new HashMap<>();
        timeOfLastUpdate = System.currentTimeMillis();
        for (long node = 0; node < graph.getNumberOfNodes(); node++) {
            nodesSortedByValue.add(new Pair<>(danf.count(node, danf.getMaxH()), node));
        }
    }

    public void updateNodeSets(Edge ... edges) {
        insertUpdatedValues(edges);
        long currentTime = System.currentTimeMillis();
        //TreeMap<Double, Long> nodesWithPercentageChange = new TreeMap<Double, Long>();
        TreeSet<Pair<Double, Long>> nodesWithPercentageChange = new TreeSet<Pair<Double, Long>>(nodeScoreComparator());
        if(currentTime - timeOfLastUpdate > UPDATE_INTERVAL_MS) {
            mergeNodeSets(nodesWithPercentageChange);
            timeOfLastUpdate = currentTime;

            if(rapidChangeCallback != null) {
                rapidChangeCallback.accept(nodesWithPercentageChange);
            }
        }
    }

    private void mergeNodeSets(TreeSet<Pair<Double, Long>> nodesWithPercentageChange) {
        Iterator<Pair<Double, Long>> it = nodesSortedByValue.iterator();
        while(it.hasNext()) {
            Pair<Double, Long> pair = it.next();
            Double currentValue = updatedNodesWithValue.get(pair.getValue());
            if(currentValue != null) { //If null, then the value haven't changed
                double previousValue = pair.getKey();
                double valueChange = currentValue / previousValue;
                if(valueChange > CHANGE_THREASHOLD_PERCENTAGE) {
                    nodesWithPercentageChange.add(new Pair(valueChange, pair.getValue()));
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

            double fromCount = danf.count(edge.from, danf.getMaxH());
            if(fromCount > MIN_NODE_COUNT) {
                updatedNodesWithValue.put(edge.from, fromCount);
            }

            double toCount = danf.count(edge.to, danf.getMaxH());
            if(toCount > MIN_NODE_COUNT) {
                updatedNodesWithValue.put(edge.to, toCount);
            }
        }
    }

    private Comparator<Pair<Double,Long>> nodeScoreComparator(){
        return (o1,o2) -> {
            int ret = o2.getKey().compareTo(o1.getKey());
            return ret == 0 ? o2.getValue().compareTo(o1.getValue()) : ret;
        };
    }
}
