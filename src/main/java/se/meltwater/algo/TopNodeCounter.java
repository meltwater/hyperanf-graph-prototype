package se.meltwater.algo;

import javafx.util.Pair;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;

import java.util.*;
import java.util.function.Consumer;

/**
 *  Provides a continuously updated TreeSet, sorted by the nodes' HLL value. Useful for maintaining
 *  the top nodes in DANF.
 *
 *  Must be updated whenever DANF is updated, else node values might be wrong.
 *
 *  Usage:
 *  danf.addEdges(newEdges);
 *  topNodeCounter.updateNodeSets(newEdges);
 *
 *  @author Simon Lindhén
 *  @author Johan Nilsson Hansen
 */
public class TopNodeCounter {

    private DANF danf;
    private TreeSet<Pair<Double, Long>> nodesSortedByValue;
    private HashMap<Long, Double> updatedNodesWithValue;
    private long timeOfLastUpdate;
    private final long updateIntervalms;
    private final double percentageChangeLimit;
    private final double minNodeCountLimit;
    private Consumer<Set<Pair<Double, Long>>> rapidChangeCallback;

    /**
     * Creates a new TopNodeCounter, where the sorted node set is updated
     * after {@code updateIntervalms} and nodes are reported as rapidly changing
     * if their danf value increase by at least {@code percentageChangeLimit}
     * and have at least {@code minNodeCountLimit} danf value.
     *
     * To use a callback function for whenever a set of rapidly changing
     * nodes have been detected, use {@link #setRapidChangeCallback(Consumer)}.
     *
     * @param danf The initiated DANF
     * @param updateIntervalms The time to update the sorted set, in milli seconds
     * @param percentageChangeLimit The percentage increase to mark a node as rapidly changing
     * @param minNodeCountLimit The least danf value a node must have to be considered for rapidly changing
     */
    public TopNodeCounter(DANF danf, long updateIntervalms, double percentageChangeLimit, long minNodeCountLimit) {
        this.danf = danf;
        initNodeSets(danf.getGraph());
        rapidChangeCallback = null;
        this.updateIntervalms = updateIntervalms;
        this.percentageChangeLimit = percentageChangeLimit;
        this.minNodeCountLimit = minNodeCountLimit;
    }

    /**
     * This callback will be called whenever a set of rapidly changing
     * nodes are detected. The argument to the callback will be the
     * percentage increase of the DANF value, along with the node id.
     * @param rapidChangeCallback
     */
    public void setRapidChangeCallback( Consumer<Set<Pair<Double, Long>>> rapidChangeCallback) {
        this.rapidChangeCallback = rapidChangeCallback;
    }

    /**
     * Adds all current nodes with values in DANF to the interally
     * sorted list of nodes/value pairs.
     * @param graph The graph containing all nodes in DANF
     */
    private void initNodeSets(IGraph graph) {
        nodesSortedByValue = new TreeSet<>(nodeScoreComparator());
        updatedNodesWithValue = new HashMap<>();
        timeOfLastUpdate = System.currentTimeMillis();
        for (long node = 0; node < graph.getNumberOfNodes(); node++) {
            nodesSortedByValue.add(new Pair<>(danf.count(node, danf.getMaxH()), node));
        }
    }

    /**
     * Updates the node/value pairs for all nodes present in an edge in {@code edges}
     * If {@code UPDATE_INTERVAL_MS} time has passed, the sorted list of nodes/values
     * is updated, else the nodes are kept in a temporal set until enough time has passed.
     * A set of the rapidly changing nodes is generated and, if set, the rapidChangeCallback is called.
     * @param edges Newly added edges
     */
    public void updateNodeSets(Edge ... edges) {
        insertUpdatedValuesToTemporalSet(edges);
        long currentTime = System.currentTimeMillis();
        TreeSet<Pair<Double, Long>> nodesWithPercentageChange = new TreeSet<>(nodeScoreComparator());
        if(currentTime - timeOfLastUpdate >= updateIntervalms) {
            mergeNodeSets(nodesWithPercentageChange);
            timeOfLastUpdate = currentTime;

            if(rapidChangeCallback != null) {
                rapidChangeCallback.accept(nodesWithPercentageChange);
            }
        }
    }

    /**
     * Merges the temporal node/value set with the full set.
     * Any node present in the temporal set is first removed
     * from the full set, and then inserted with its new value.
     * If the value have changed more than a certain percentage,
     * the node/value pair is added to {@code nodesWithPercentageChange}
     * @param nodesWithPercentageChange A set to add rapidly changing nodes to
     */
    private void mergeNodeSets(TreeSet<Pair<Double, Long>> nodesWithPercentageChange) {
        Iterator<Pair<Double, Long>> it = nodesSortedByValue.iterator();
        while(it.hasNext()) {
            Pair<Double, Long> pair = it.next();
            Double currentValue = updatedNodesWithValue.get(pair.getValue());
            if(currentValue != null) { //If null, then the value haven't changed
                double previousValue = pair.getKey();
                double valueChange = currentValue / previousValue;
                if(valueChange > percentageChangeLimit && previousValue > minNodeCountLimit) {
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

    /**
     * Inserts all nodes in {@code edges} in to the temporal set, along with
     * their danf values.
     * @param edges
     */
    private void insertUpdatedValuesToTemporalSet(Edge ... edges) {
        for (int i = 0; i < edges.length; i++) {
            Edge edge = edges[i];

            updatedNodesWithValue.put(edge.from, danf.count(edge.from, danf.getMaxH()));
            updatedNodesWithValue.put(edge.to, danf.count(edge.to, danf.getMaxH()));
        }
    }

    /**
     * Returns a comparator that sorts on the key but
     * equals on the value.
     * @return
     */
    private Comparator<Pair<Double,Long>> nodeScoreComparator(){
        return (o1,o2) -> {
            int ret = o2.getKey().compareTo(o1.getKey());
            return ret == 0 ? o2.getValue().compareTo(o1.getValue()) : ret;
        };
    }

    public void close(){
        danf.close();
    }

}
