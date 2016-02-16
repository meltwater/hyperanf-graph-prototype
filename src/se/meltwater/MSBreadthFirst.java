package se.meltwater;

import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import sun.nio.ch.ThreadPool;

import java.io.IOException;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by simon on 2016-02-15.
 */
public class MSBreadthFirst {

    private ImmutableGraph graph;
    private int[] bfsSources;
    private int numSources;

    public static void main(String[] args) throws IOException, InterruptedException {

        BVGraph graph = BVGraph.load("graphs/uk-2007-05@100000");
        int[] bfsSources = new int[1000];
        Random rand = new Random();
        for(int i=0; i<1000 ; i++)
            bfsSources[i] = rand.nextInt(graph.numNodes());
        new MSBreadthFirst(bfsSources,graph).ownBreadthFirstSearch();

    }

    public MSBreadthFirst(int[] bfsSources, ImmutableGraph graph){

        this.graph = graph;
        numSources = bfsSources.length;
        this.bfsSources = bfsSources;

    }

    public void breadthFirstSearch(){

        BitSet visit = new BitSet(numSources*graph.numNodes()), seen = new BitSet(numSources*graph.numNodes());

        for(int bfs = 0; bfs < numSources; bfs++){
            int bfsBit = bfs+bfsSources[bfs]*numSources;
            seen.set(bfsBit);
            visit.set(bfsBit);
        }

        System.out.println("Starting Breadth first");
        long time = System.currentTimeMillis();

        MSBFS(visit,seen);
        System.out.println("Finished Breadth first in: " + (System.currentTimeMillis() - time) + "ms");

    }

    public void ownBreadthFirstSearch() throws InterruptedException {

        BitSet visit = new BitSet(graph.numNodes());

        for(int node = 0; node < graph.numNodes(); node++){
            visit.set(node);
        }

        System.out.println("Starting Breadth first");
        long time = System.currentTimeMillis();

        BitSet seen = ownBFS(visit);
        System.out.println("Finished Breadth first in: " + (System.currentTimeMillis() - time) + "ms");

    }

    private BitSet ownBFS(BitSet visit) throws InterruptedException {
        int numOfBits = graph.numNodes();
        BitSet seen = (BitSet)visit.clone();
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(processors);
        while (visit.cardinality() > 0){
            BitSet visitNext = new BitSet(numOfBits);

            for(int i = 0; i<processors ; i++)
                pool.execute(bfsIteration(i*numOfBits/processors,(i+1)*numOfBits/processors,visitNext,seen,visit));

            pool.shutdown();
            pool.awaitTermination(2, TimeUnit.HOURS);
            visit = visitNext;
        }
        return seen;
    }

    private Runnable bfsIteration(int from, int to, BitSet visitNext, BitSet seen, BitSet visit){
        return () -> {
            int startAt = from,node;
            while((node = visit.nextSetBit(startAt++)) != -1 && node < to) {

                LazyIntIterator neighbors = graph.successors(node);
                int degree = graph.outdegree(node);
                int neighbor;
                seen.set(node);
                for (int d = 0; d < degree; d++) {
                    neighbor = neighbors.nextInt();
                    if(!seen.get(neighbor))
                        visitNext.set(neighbor);
                }

            }
        };
    }

    private void MSBFS(BitSet visit, BitSet seen){

        int numOfBits = numSources*graph.numNodes();
        BitSet visitNext = new BitSet(numOfBits);
        while(visit.cardinality() > 0){
            int startAt = 0,firstOne;
            while(startAt < (numOfBits-numSources) && (firstOne = visit.nextSetBit(startAt)) != -1) {

                int node = firstOne / numSources;
                int startBit = node * numSources, endBit = startBit + numSources;
                startAt = endBit;
                BitSet nodeVisits = visit.get(startBit, endBit);

                LazyIntIterator neighbors = graph.successors(node);
                int degree = graph.outdegree(node);
                int neighbor;
                for (int d = 0; d < degree; d++) {
                    neighbor = neighbors.nextInt();
                    int neighStartBit = neighbor * numSources, neighEndBit = neighStartBit + numSources;
                    BitSet seenConjugates = seen.get(neighStartBit, neighEndBit);
                    seenConjugates.flip(0, numSources);
                    BitSet newVisits = (BitSet) nodeVisits.clone();
                    newVisits.and(seenConjugates);
                    if (newVisits.cardinality() > 0) {
                        BitSet actualNewVisits = new BitSet(numOfBits);
                        for (int bit = 0; bit < numSources; bit++)
                            actualNewVisits.set(bit + neighStartBit, newVisits.get(bit));
                        visitNext.or(actualNewVisits);
                        seen.or(actualNewVisits);
                    }

                }
            }
            visit = (BitSet) visitNext.clone();
            visitNext.clear();
        }

    }


}
