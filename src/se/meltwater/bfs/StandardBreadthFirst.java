package se.meltwater.bfs;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import se.meltwater.graph.IGraph;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class StandardBreadthFirst {

    public static void breadthFirstSearch(long[] sources, IGraph graph, int maxSteps) {
        for (int bfs = 0; bfs < sources.length ; bfs++) {
            int h = 0;
            LongArrayFIFOQueue currentQueue = new LongArrayFIFOQueue();
            LongArrayFIFOQueue nextQueue = new LongArrayFIFOQueue();
            currentQueue.enqueue(sources[bfs]);
            LongArrayBitVector nodesChecked = LongArrayBitVector.ofLength(graph.getNumberOfNodes());
            nodesChecked.set(sources[bfs]);

            /* Do bfs from current node */
            while (!currentQueue.isEmpty() && h <= maxSteps) {
                long curr = currentQueue.dequeueLong();
                LazyLongIterator succs = graph.getSuccessors(curr);
                long d = graph.getOutdegree(curr);

                /* Visit all neighbors */
                while (d-- != 0) {
                    long succ = succs.nextLong();

                    if (!nodesChecked.get(succ)) {
                        nodesChecked.set(succ);
                        nextQueue.enqueue(succ);
                    }
                }

                if(currentQueue.isEmpty()) {
                    currentQueue = nextQueue;
                    nextQueue = new LongArrayFIFOQueue();
                    h++;
                }
            }
        }
    }
}
