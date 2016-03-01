package se.meltwater;

import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.examples.BreadthFirst;
import sun.nio.ch.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
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

        BVGraph graph = BVGraph.load("files/uk-2007-05@100000");
        int[] bfsSources = new int[1000];
        Random rand = new Random();
        for(int i=0; i<1000 ; i++)
            bfsSources[i] = rand.nextInt(graph.numNodes());
        new MSBreadthFirst(bfsSources,graph).breadthFirstSearch();

    }

    public MSBreadthFirst(int[] bfsSources, ImmutableGraph graph){

        this.graph = graph;
        numSources = bfsSources.length;
        this.bfsSources = bfsSources;

    }

    private BitSet[] createBitsets(){
        BitSet[] list = new BitSet[graph.numNodes()];
        for(int node = 0; node < graph.numNodes() ; node++)
            list[node] = new BitSet(bfsSources.length);
        return list;
    }

    public void breadthFirstSearch() throws InterruptedException {

        BitSet[] visit = createBitsets(), seen = createBitsets();

        for(int bfs = 0; bfs < numSources; bfs++){
            visit[bfsSources[bfs]].set(bfs);
            seen[bfsSources[bfs]].set(bfs);
        }

        System.out.println("Starting Breadth first");
        long time = System.currentTimeMillis();

        MSBFS(visit,seen);
        System.out.println("Finished Breadth first in: " + (System.currentTimeMillis() - time) + "ms");

    }

    public void ownBreadthFirstSearch() throws InterruptedException {

        BitSet visit = new BitSet(graph.numNodes());
        System.out.println("Starting Breadth first");
        long time = System.currentTimeMillis();
        try {
            for (int node : bfsSources) {
                //visit.set(node);
                BreadthFirst.main(new String[]{"-s", node + "", "files/uk-2007-05@100000"});
            }
        }catch (Exception e ){
            e.printStackTrace();
        }


        //BitSet seen = ownBFS(visit);
        System.out.println("Finished Breadth first in: " + (System.currentTimeMillis() - time) + "ms");

    }

    private BitSet ownBFS(BitSet visit) throws InterruptedException {
        int numOfBits = graph.numNodes();
        BitSet seen = (BitSet)visit.clone();
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(processors);
        while (visit.cardinality() > 0){
            BitSet visitNext = new BitSet(numOfBits);

            int bitsPerProcessor = numOfBits / processors;
            for(int i = 0; i<processors ; i++) {
                int startAt = i * bitsPerProcessor;
                int endAt = i < processors-1 ? startAt + bitsPerProcessor : numOfBits;
                pool.execute(bfsIteration(startAt, endAt, visitNext, seen, visit));
            }

            pool.shutdown();
            pool.awaitTermination(2, TimeUnit.HOURS);
            pool = Executors.newFixedThreadPool(processors);
            visit = visitNext;
            System.out.println("Iteration finished");
        }
        return seen;
    }

    private Runnable bfsIteration(int from, int to, BitSet visitNext, BitSet seen, BitSet visit){
        return () -> {
            int startAt = from,node;
            while((node = visit.nextSetBit(startAt++)) != -1 && node < to) {

                LazyIntIterator neighbors;
                int degree;
                synchronized (this) {
                    neighbors = graph.successors(node);
                    degree = graph.outdegree(node);
                }
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

    private void MSBFS(BitSet[] visit, BitSet[] seen) throws InterruptedException {

        BitSet[] visitNext = createBitsets();
        BoolWrapper visitHadContent = new BoolWrapper(true);
        int processors = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(processors);
        int nodesPerProcessor = graph.numNodes() / processors;
        while(visitHadContent.theBool){
            visitHadContent.theBool = false;

            for(int i = 0; i < processors; i++) {
                int start = i*nodesPerProcessor;
                int end = i == processors-1 ? graph.numNodes() : start + nodesPerProcessor;
                pool.submit(firstPhaseIterator(start,end,visit,visitNext,visitHadContent));
            }
            pool.shutdown();
            pool.awaitTermination(1,TimeUnit.MINUTES);
            pool = Executors.newFixedThreadPool(processors);

            if(visitHadContent.theBool) {

                for(int i = 0; i < processors; i++) {
                    int start = i*nodesPerProcessor;
                    int end = i == processors-1 ? graph.numNodes() : start + nodesPerProcessor;
                    pool.submit(secondPhaseIterator(start,end,visitNext,seen));
                }
                pool.shutdown();
                pool.awaitTermination(1,TimeUnit.MINUTES);
                pool = Executors.newFixedThreadPool(processors);

                visit = visitNext;
                visitNext = createBitsets();
            }
            System.out.println("Finished iteration.");
        }

    }

    private Runnable firstPhaseIterator(int startNode, int endNode, BitSet[] visit, BitSet[] visitNext, BoolWrapper visitHadContent){
        return () -> {
            NodeIterator nodeIt = graph.nodeIterator(startNode);
            for(int node = startNode; node < endNode ; node++) {
                nodeIt.nextInt();
                if (visit[node].cardinality() == 0) continue;

                visitHadContent.theBool = true;
                LazyIntIterator neighbors = nodeIt.successors();
                int degree = nodeIt.outdegree();
                int neighbor;
                for (int d = 0; d < degree; d++) {
                    neighbor = neighbors.nextInt();
                    visitNext[neighbor].or(visit[node]);

                }

            }

        };
    }

    private Runnable secondPhaseIterator(int startNode, int endNode, BitSet[] visitNext, BitSet[] seen){
        return () -> {
            for(int node = startNode; node < endNode ; node++) {
                if(visitNext[node].cardinality() == 0) continue;

                visitNext[node].andNot(seen[node]);
                seen[node].or(visitNext[node]);
            }
        };
    }

    /*private void MSBFS(BitSet[] visit, BitSet[] seen){

        BitSet[] visitNext = createBitsets();
        boolean visitHadContent = true;
        while(visitHadContent){
            visitHadContent = false;
            for(int node = 0; node < visit.length ; node++) {
                if (visit[node].cardinality() == 0) continue;

                visitHadContent = true;
                LazyIntIterator neighbors = graph.successors(node);
                int degree = graph.outdegree(node);
                int neighbor;
                for (int d = 0; d < degree; d++) {
                    neighbor = neighbors.nextInt();
                    BitSet D = (BitSet) visit[node].clone();
                    D.andNot(seen[neighbor]);
                    if (D.cardinality() > 0) {
                        visitNext[neighbor].or(D);
                        seen[neighbor].or(D);
                    }

                }

            }

            System.out.println("Finished iteration.");
            if(visitHadContent) {
                visit = visitNext;
                visitNext = createBitsets();
            }
        }

    }*/

    class BoolWrapper{
        public boolean theBool = false;
        public BoolWrapper(boolean initialValue){ theBool = initialValue; }
        public BoolWrapper(){this(false);}
    }


}
