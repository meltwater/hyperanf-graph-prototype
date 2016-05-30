package it.unimi.dsi.big.webgraph.algo;

import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.big.webgraph.MutableGraph;

import java.util.concurrent.*;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class StandardBreadthFirst {

    private ExecutorService threadPool;
    private final Integer syncInt = 0;
    private final int nrThreads;


    public StandardBreadthFirst() {
        nrThreads = Runtime.getRuntime().availableProcessors();
    }



    public void breadthFirstSearch(long[] sources, MutableGraph graph, int maxSteps) {
        threadPool = Executors.newFixedThreadPool(nrThreads);

        for (int i = 0; i <  sources.length; i++) {
            final int sourceIndex = i;
            threadPool.submit(() -> {
                long currentNode = sources[sourceIndex];

                LongArrayFIFOQueue currentQueue = new LongArrayFIFOQueue();
                LongArrayFIFOQueue nextQueue = new LongArrayFIFOQueue();
                currentQueue.enqueue(currentNode);
                LongArrayBitVector nodesChecked = LongArrayBitVector.ofLength(graph.numNodes());
                nodesChecked.set(currentNode);
                int h = 0;

                while (h <= maxSteps) {
                    long curr = currentQueue.dequeueLong();

                    long d ;
                    long[][] successors;
                    NodeIterator currentNodeIterator;
                    synchronized (StandardBreadthFirst.this) {
                        currentNodeIterator = graph.nodeIterator(curr);
                    }
                        currentNodeIterator.nextLong();
                        d = currentNodeIterator.outdegree();
                        successors = currentNodeIterator.successorBigArray();

                    /* Visit all neighbors */
                    while (d != 0) {
                        long succ = LongBigArrays.get(successors, d-1);

                        if (!nodesChecked.get(succ)) {
                            nodesChecked.set(succ);
                            nextQueue.enqueue(succ);
                        }
                        d--;
                    }

                    if (currentQueue.isEmpty()) {
                        currentQueue = nextQueue;
                        nextQueue = new LongArrayFIFOQueue();
                        h++;
                    }
                }
            });
        }

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(2, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
