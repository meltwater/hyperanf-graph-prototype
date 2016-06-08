package it.unimi.dsi.big.webgraph.algo;

import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.logging.ProgressLogger;
import javafx.util.Pair;

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
    private BoundedTreeSet<Pair<Double, Long>> nodesSortedByValue;

    private HashMap<Long, Double> updatedNodesWithValueBeforeUpdate;
    private HashMap<Long, Double> updatedNodesWithValueAfterUpdate;
    private HashMap<Long, Double> rapidlyChangingNodes;

    private long timeOfLastUpdate;
    private final long updateIntervalms;
    private final double percentageChangeLimit;
    private final double minNodeCountLimit;
    private Consumer<Set<Pair<Double, Long>>> rapidChangeCallback;
    private final int counterCapacity;

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
    public TopNodeCounter(DANF danf, long updateIntervalms, double percentageChangeLimit, long minNodeCountLimit, int counterCapacity) {
        this.danf = danf;
        rapidChangeCallback = null;
        this.updateIntervalms = updateIntervalms;
        this.percentageChangeLimit = percentageChangeLimit;
        this.minNodeCountLimit = minNodeCountLimit;
        this.counterCapacity = counterCapacity;

        initNodeSets(danf.getGraph());
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
    private void initNodeSets(MutableGraph graph) {
        System.out.println("Initing TopNodes");
        ProgressLogger pl = new ProgressLogger();
        pl.expectedUpdates = graph.numNodes();

        nodesSortedByValue = new BoundedTreeSet<>(counterCapacity ,nodeScoreComparator());
        updatedNodesWithValueBeforeUpdate = new HashMap<>();
        updatedNodesWithValueAfterUpdate = new HashMap<>();
        rapidlyChangingNodes = new HashMap<>();
        timeOfLastUpdate = System.currentTimeMillis();

        pl.start();
        for (long node = 0; node < graph.numNodes(); node++) {
            nodesSortedByValue.add(new Pair<>(danf.count(node, danf.getMaxH()), node));
            pl.update(1);
        }
        pl.done();
        pl.logger.info( pl.toString() );
    }

    /**
     * Updates the node/value pairs for all nodes present in an edge in {@code edges}
     * If {@code UPDATE_INTERVAL_MS} time has passed, the sorted list of nodes/values
     * is updated, else the nodes are kept in a temporal set until enough time has passed.
     * A set of the rapidly changing nodes is generated and, if set, the rapidChangeCallback is called.
     * @param edges Newly added edges
     */
    public void updateNodeSetsAfter(Edge ... edges) {
        insertUpdatedValuesToTemporalSet(updatedNodesWithValueAfterUpdate, true, edges);

        long currentTime = System.currentTimeMillis();
        if(currentTime - timeOfLastUpdate >= updateIntervalms) {
            mergeNodeSets();
            TreeSet<Pair<Double, Long>> nodesWithPercentageChange = getRapidlyChangingNodes();

            nodesWithPercentageChange.stream().forEach(pair -> {
                Double value = rapidlyChangingNodes.get(pair.getValue());
                if(value == null) {
                    rapidlyChangingNodes.put(pair.getValue(), pair.getKey());
                } else {
                    rapidlyChangingNodes.put(pair.getValue(), pair.getKey() * value);
                }
            });

            timeOfLastUpdate = currentTime;

            if(rapidChangeCallback != null) {
                rapidChangeCallback.accept(nodesWithPercentageChange);
            }
        }
    }

    public HashMap<Long, Double> getRapidlyChangedNodes() {
        HashMap<Long, Double> ret = rapidlyChangingNodes;
        rapidlyChangingNodes = new HashMap<>();
        return ret;
    }


    public void updateNodeSetsBefore(Edge ... edges) {
        insertUpdatedValuesToTemporalSet(updatedNodesWithValueBeforeUpdate, false, edges);
    }

    /**
     * Merges the temporal node/value set with the full set.
     * Any node present in the temporal set is first removed
     * from the full set, and then inserted with its new value.
     */
    private void mergeNodeSets() {
        Iterator<Pair<Double, Long>> it = nodesSortedByValue.iterator();
        while(it.hasNext()) {
            Pair<Double, Long> pair = it.next();
            Double currentValue = updatedNodesWithValueAfterUpdate.get(pair.getValue());
            if(currentValue != null) { //If null, then the value haven't changed
                it.remove();
            }
        }

        updatedNodesWithValueAfterUpdate.forEach((key, value) -> nodesSortedByValue.add(new Pair<>(value, key)));
    }

    private TreeSet<Pair<Double, Long>> getRapidlyChangingNodes() {
        TreeSet<Pair<Double, Long>> nodesWithPercentageChange = new TreeSet<>(nodeScoreComparator());

        updatedNodesWithValueAfterUpdate.forEach( (node, newValue) -> {
            double oldValue = updatedNodesWithValueBeforeUpdate.get(node);
            double percentageChange = newValue / oldValue;
            if(percentageChange > percentageChangeLimit && oldValue >= minNodeCountLimit) {
                nodesWithPercentageChange.add(new Pair<>(percentageChange, node));
            }
        });

        updatedNodesWithValueAfterUpdate.clear();
        updatedNodesWithValueBeforeUpdate.clear();

        return nodesWithPercentageChange;
    }

    public TreeSet<Pair<Double, Long>> getNodesSortedByValue() {
        return nodesSortedByValue;
    }

    /**
     * Inserts all nodes in {@code edges} in to the temporal set, along with
     * their danf values.
     * @param edges
     */
    private void insertUpdatedValuesToTemporalSet(Map<Long, Double> map, boolean overrideOldValues, Edge ... edges) {
        for (int i = 0; i < edges.length; i++) {
            Edge edge = edges[i];

            addLongDoublePairToMap(map, overrideOldValues, edge.from);
            addLongDoublePairToMap(map, overrideOldValues, edge.to);
        }
    }

    private void addLongDoublePairToMap(Map<Long, Double> map, boolean overrideOldValues, long node) {
        if(overrideOldValues || !map.containsKey(node)) {
            double value;
            try {
                value = danf.count(node, danf.getMaxH());
            } catch (IllegalArgumentException e ) {
                value = 1; //All nodes have 1 value in the beginning
            }
            map.put(node, value);
        }
    }

    /**
     * Returns a comparator that sorts on the key but
     * equals on the value.
     * @return
     */
    public static Comparator<Pair<Double,Long>> nodeScoreComparator(){
        return (o1,o2) -> {
            int ret = o2.getKey().compareTo(o1.getKey());
            return ret == 0 ? o2.getValue().compareTo(o1.getValue()) : ret;
        };
    }

    public void close(){
        danf.close();
    }

}
