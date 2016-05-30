package it.unimi.dsi.big.webgraph.algo;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.HyperLogLogCounterArray;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.big.webgraph.Utils;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * A dynamic approximate neighborhood function.
 *
 * <b>Warning:</b> This implementation is very slow and was only implemented to be compared
 * to DANF. Use DANF if you want to calculate the approximate neighborhood function dynamically.
 * @see DANF
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class TrivialDynamicANF implements DynamicNeighborhoodFunction{

    private int h;
    private HyperLogLogCounterArray counters;
    private MutableGraph graph, transposeGraph;

    /**
     *
     * <b>Warning:</b> This implementation is very slow and was only implemented to be compared
     * to DANF. Use DANF if you want to calculate the approximate neighborhood function dynamically.
     *
     * @see DANF
     *
     * @param h
     * @param log2m
     * @param graph
     */
    public TrivialDynamicANF(int h, int log2m, MutableGraph graph){
        this(h,log2m,graph, Util.randomSeed());
    }

    /**
     *
     * <b>Warning:</b> This implementation is very slow and was only implemented to be compared
     * to DANF. Use DANF if you want to calculate the approximate neighborhood function dynamically.
     *
     * @see DANF
     *
     * @param h
     * @param log2m
     * @param graph
     */
    public TrivialDynamicANF(int h, int log2m, MutableGraph graph, long seed){

        this.graph = graph;
        transposeGraph = graph.transpose();
        this.h = h;

        HyperBall hb = new HyperBall(graph, transposeGraph,log2m,seed);
        try {
            hb.run(h);
            hb.close();
        } catch (IOException e) {
            throw new RuntimeException("Should never happen as it's not external",e);
        }
        counters = hb.getCounter();

    }

    @Override
    public long getMemoryUsageBytes() {
        return Utils.getMemoryUsage(counters) + graph.getMemoryUsageBytes() + transposeGraph.getMemoryUsageBytes();
    }


    /**
     *
     * Returns the graph which the neighborhood function is calculated on.
     *
     * <b>Warning:</b> This implementation is very slow and was only implemented to be compared
     * to DANF. Use DANF if you want to calculate the approximate neighborhood function dynamically.
     *
     * @see DANF
     *
     */
    public MutableGraph getGraph(){
        return graph;
    }

    /**
     *
     * Returns the approximate neighborhood function of the given node
     *
     * <b>Warning:</b> This implementation is very slow and was only implemented to be compared
     * to DANF. Use DANF if you want to calculate the approximate neighborhood function dynamically.
     *
     * @see DANF
     *
     * @param node
     */
    public double count(long node){
        return counters.count(node);
    }

    /**
     *
     * Adds the given edges to the graph and updates the neighborhood function.
     *
     * <b>Warning:</b> This implementation is very slow and was only implemented to be compared
     * to DANF. Use DANF if you want to calculate the approximate neighborhood function dynamically.
     *
     * @see DANF
     *
     * @param edges
     */
    public void addEdges(Edge... edges){
        Edge[] flipped = new Edge[edges.length];
        long maxNode = 0;
        for (int i = 0; i < edges.length; i++) {
            flipped[i] = edges[i].flip();
            maxNode = Math.max(maxNode,Math.max(edges[i].from, edges[i].to));
        }
        if(!graph.containsNode(maxNode)){
            counters.addCounters(maxNode+1 - graph.numNodes());
            for(long node = graph.numNodes(); node <= maxNode; node++)
                counters.add(node,node);
        }
        graph.addEdges(edges);
        transposeGraph.addEdges(flipped);
        ProgressLogger pl = new ProgressLogger();
        pl.itemsName = "Edges";
        pl.expectedUpdates = edges.length;
        pl.start();
        for (Edge edge : edges) {
            addEdge(edge);
            pl.update();
        }
        pl.stop();
    }

    @Override
    public void close() {}

    private void addEdge(Edge edge){
        long[][] toAdd = collectHistory(edge.to);
        propagateHistory(edge.from,toAdd);
    }

    private long[][] collectHistory(long to) {
        long[][] toAdd = new long[h][counters.counterLongwords];
        bfs(graph,to,(node, i) -> counters.add(node,toAdd[i]));
        for(int i = 1; i < h; i++)
            counters.max(toAdd[i],toAdd[i-1]);
        return toAdd;
    }

    private void propagateHistory(long from, long[][] history){
        bfs(transposeGraph,from,(node, i) -> {
            long[] nodeCounter = new long[counters.counterLongwords];
            counters.getCounter(node,nodeCounter);
            counters.max(nodeCounter,history[h-i-1]);
            counters.setCounter(nodeCounter,node);
        });
    }

    private void bfs(MutableGraph graph, long source, BiConsumer<Long,Integer> onVisit){
        LongArrayFIFOQueue queue = new LongArrayFIFOQueue();
        LongArrayFIFOQueue nextQueue = new LongArrayFIFOQueue();
        LongArrayBitVector seen = LongArrayBitVector.ofLength(graph.numNodes());
        queue.enqueue(source);
        int i = 0;
        while(i < h){
            while (!queue.isEmpty()) {
                long node = queue.dequeueLong();
                onVisit.accept(node,i);

                if(i+1 < h) {
                    NodeIterator nodeIt = graph.nodeIterator(node);
                    nodeIt.nextLong();
                    long degree = nodeIt.outdegree();
                    LazyLongIterator successors = nodeIt.successors();
                    while (degree-- > 0) {
                        long neigh = successors.nextLong();
                        if (!seen.getBoolean(neigh)) {
                            nextQueue.enqueue(neigh);
                            seen.set(neigh);
                        }
                    }
                }
            }
            queue = nextQueue;
            nextQueue = new LongArrayFIFOQueue();
            i++;
        }

    }

}
