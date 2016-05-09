package se.meltwater.algo;

import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * TODO Class description
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class TrivialDynamicANF implements DynamicNeighborhoodFunction{

    private int h;
    private HyperLolLolCounterArray counters;
    private IGraph graph, transposeGraph;

    public TrivialDynamicANF(int h, int log2m, IGraph graph){
        this(h,log2m,graph, Util.randomSeed());
    }

    public TrivialDynamicANF(int h, int log2m, IGraph graph, long seed){

        this.graph = graph;
        transposeGraph = graph.transpose();
        this.h = h;

        HyperBoll hb = new HyperBoll(graph, transposeGraph,log2m,seed);
        try {
            hb.run(h);
            hb.close();
        } catch (IOException e) {
            throw new RuntimeException("Should never happen as it's not external",e);
        }
        counters = hb.getCounter();

    }

    public IGraph getGraph(){
        return graph;
    }

    public double count(long node){
        return counters.count(node);
    }

    public void addEdges(Edge... edges){
        Edge[] flipped = new Edge[edges.length];
        long maxNode = 0;
        for (int i = 0; i < edges.length; i++) {
            flipped[i] = edges[i].flip();
            maxNode = Math.max(maxNode,Math.max(edges[i].from, edges[i].to));
        }
        if(!graph.containsNode(maxNode)){
            counters.addCounters(maxNode+1 - graph.getNumberOfNodes());
            for(long node = graph.getNumberOfNodes(); node <= maxNode; node++)
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

    public void addEdge(Edge edge){
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

    private void bfs(IGraph graph, long source, BiConsumer<Long,Integer> onVisit){
        LongArrayFIFOQueue queue = new LongArrayFIFOQueue();
        LongArrayFIFOQueue nextQueue = new LongArrayFIFOQueue();
        LongArrayBitVector seen = LongArrayBitVector.ofLength(graph.getNumberOfNodes());
        queue.enqueue(source);
        int i = 0;
        while(i < h){
            while (!queue.isEmpty()) {
                long node = queue.dequeueLong();
                onVisit.accept(node,i);

                if(i+1 <= h) {
                    NodeIterator nodeIt = graph.getNodeIterator(node);
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
