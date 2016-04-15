package se.meltwater.bfs;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import se.meltwater.graph.IGraph;

import java.util.*;
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
    private int threadsLeft;
    private final int maxThreads;
    private volatile int activeThreads;

    private SortedSet<Long> nodesToGetIteratorFor;
    private ConcurrentHashMap<Long, NodeIterator> nodeIterators;

    public StandardBreadthFirst() {
        int nrThreads = 2;//Runtime.getRuntime().availableProcessors();
        threadPool = Executors.newFixedThreadPool(nrThreads);
        threadsLeft = nrThreads;
        maxThreads = nrThreads;
        activeThreads = nrThreads;

        nodesToGetIteratorFor = Collections.synchronizedSortedSet(new TreeSet<>());
        nodeIterators = new ConcurrentHashMap<>();
    }


    private void fetchIteratorsOrWait(IGraph graph) {
        synchronized (syncInt) {
            if (--threadsLeft == 0) {

                nodesToGetIteratorFor.forEach(node -> {
                    System.out.println("Fetching iterator for node: " + node);
                    NodeIterator nodeIterator = graph.getNodeIterator(node);
                    nodeIterator.nextLong();
                    nodeIterators.put(node, nodeIterator);
                });

                nodesToGetIteratorFor.clear();

                threadsLeft = activeThreads;
                syncInt.notifyAll();
            } else {
                try {
                    System.out.println("Thread " + Thread.currentThread().getId() + " is now waiting for other threads");
                    syncInt.wait();
                    System.out.println("Thread " + Thread.currentThread().getId() + " is now alive again");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(31);
                }
            }
        }
    }

    public void breadthFirstSearch(long[] sources, IGraph graph, int maxSteps) {
        for (int i = 0; i < sources.length; i++) {
            nodesToGetIteratorFor.add(sources[i]);
        }

        final int threadBulkSize = sources.length / maxThreads;
        ArrayList<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < maxThreads ; i++) {
            final int finalI = i;

            futures.add(threadPool.submit(() -> {

                int startBfsIndex = finalI * threadBulkSize;
                int endBfsIndex = Math.min(startBfsIndex + threadBulkSize, sources.length);
                int currentIndex = startBfsIndex;

                while(currentIndex < endBfsIndex) {
                    long currentNode = sources[currentIndex];

                    int h = 0;
                    LongArrayFIFOQueue currentQueue = new LongArrayFIFOQueue();
                    LongArrayFIFOQueue nextQueue = new LongArrayFIFOQueue();
                    currentQueue.enqueue(currentNode);
                    LongArrayBitVector nodesChecked = LongArrayBitVector.ofLength(graph.getNumberOfNodes());
                    nodesChecked.set(currentNode);



             /* Do bfs from current node */
                    while (h <= maxSteps) {
                        fetchIteratorsOrWait(graph);

                        if(currentQueue.isEmpty()) {
                            h++;
                            continue;
                        }

                        long curr = currentQueue.dequeueLong();
                        NodeIterator currentNodeIterator = nodeIterators.get(curr);
                        long d = currentNodeIterator.outdegree();

                        long[][] successors;
                        synchronized (StandardBreadthFirst.this) {
                            successors = currentNodeIterator.successorBigArray();
                        }

                /* Visit all neighbors */
                        while (d != 0) {
                            long succ = LongBigArrays.get(successors, d-1);

                            if (!nodesChecked.get(succ)) {
                                nodesChecked.set(succ);
                                nextQueue.enqueue(succ);

                                if (!nodeIterators.containsKey(succ)) {
                                    nodesToGetIteratorFor.add(succ);
                                }
                            }
                            d--;
                        }

                        if (currentQueue.isEmpty()) {
                            currentQueue = nextQueue;
                            nextQueue = new LongArrayFIFOQueue();
                            h++;
                            System.out.println("Thread " + Thread.currentThread().getId() + ". h is now: " + h);
                        }
                    }

                    currentIndex++;
                }

                synchronized (syncInt) {
                    activeThreads--;
                    if(--threadsLeft == 0) {
                        syncInt.notifyAll();
                    }
                }
                System.out.println("Thread " + Thread.currentThread().getId() + " is now finished");
            }));
        }

        try {
            futures.forEach(future -> {
                try {
                    future.get(); // raises ExecutionException for any uncaught exception in child
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(0);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            });

        } catch (Exception e) {
            System.out.println("** RuntimeException from thread ");
            e.getCause().printStackTrace(System.out);
            System.exit(0);
        }

        threadPool.shutdown();
        try {
            threadPool.awaitTermination(100, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
