package se.meltwater.bfs;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import se.meltwater.graph.IGraph;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * A class to perform Multi-source Breadth-first searches on graphs using the algorithm
 * developed in the paper: "The More the Merrier: Efficient Multi-Source Graph Traversal"
 */
public class MSBreadthFirst {

    private IGraph graph;
    private int numSources;
    private int threadsLeft;
    private Visitor visitor;
    private Traveler[][] travelers;
    private Traveler[][] travelersNext;
    private boolean threadFailure;

    private ExecutorService threadManager;
    private int threads;
    private static AtomicInteger threadFactoryID = new AtomicInteger(0);

    private AtomicBoolean visitHadContent;
    private BitSet[][] visit;
    private BitSet[][] seen;
    private BitSet[][] visitNext;
    private int iteration;
    private boolean hasTraveler;

    /**
     * Initialize a Breadth-first search in {@code graph}.
     *
     * <b>WARNING:</b> Not calling {@link MSBreadthFirst#close()} can quickly eat up
     * the Heap due to idle threads.
     * @param graph
     */
    public MSBreadthFirst(IGraph graph){
        this.graph = graph;
        threads = Runtime.getRuntime().availableProcessors() * 2;
        threadManager = Executors.newFixedThreadPool(threads, new MSBreadthFirstThreadFactory(threadFactoryID.getAndIncrement()));
    }

    /**
     * Shuts down the threads maintained by this object.
     */
    public void close(){
        if(!threadManager.isShutdown()) {
            threadManager.shutdownNow();
        }
    }

    private BitSet[][] createBitsets(){
        BitSet[][] list = ObjectBigArrays.newBigArray(new BitSet[0][0],graph.getNumberOfNodes());
        /*for(long node = 0; node < graph.getNumberOfNodes() ; node++)
            ObjectBigArrays.set(list,node,null);*/
        return list;
    }

    private void fillWithNewBitSets(BitSet[][] bsets){
        for(long node = 0; node < graph.getNumberOfNodes() ; node++)
            ObjectBigArrays.set(bsets,node,null);
    }

    public BitSet[][] breadthFirstSearch(long[] bfsSources, Visitor visitor) throws InterruptedException {
        return breadthFirstSearch(bfsSources,visitor,null);
    }

    public BitSet[][] breadthFirstSearch(long[] bfsSources) throws InterruptedException {
        return breadthFirstSearch(bfsSources,null);
    }

    /**
     * Performs a Multi-Source Breadth-first search.
     *
     * If a visitor has been specified it will be called as follows:
     * Each time a new node is visited the visitor will be called for that node.
     * @see MSBreadthFirst.Visitor
     *
     * Note that the visitors are likely to be called in parallel
     * @return An array of BitSets where the index in the list specifies the node and the
     * set bits indicate which bfs's that reached it.
     * @throws InterruptedException
     */
    public BitSet[][] breadthFirstSearch(long[] bfsSources, Visitor visitor, Traveler[] travelers) throws InterruptedException {

        numSources = bfsSources.length;
        this.visitor = visitor;
        hasTraveler = travelers != null;
        if(travelers == null) {
            this.travelers = null;
            this.travelersNext = null;
        }else {
            this.travelers = ObjectBigArrays.newBigArray(new Traveler[0][0], graph.getNumberOfNodes());
            travelersNext = ObjectBigArrays.newBigArray(new Traveler[0][0], graph.getNumberOfNodes());
        }
        threadFailure = false;
        visit = createBitsets();
        seen = createBitsets();

        Traveler temp;
        for(int bfs = 0; bfs < numSources; bfs++){
            long node = bfsSources[bfs];
            setBfs(visit,node,bfs);
            setBfs(seen,node,bfs);
            if(hasTraveler) {
                temp = ObjectBigArrays.get(this.travelers,node);
                temp = temp == null ? travelers[bfs] : temp.merge(travelers[bfs],0);
                ObjectBigArrays.set(this.travelers,node,temp);
                ObjectBigArrays.set(travelersNext,node,temp);
            }
        }

        MSBFS();

        return seen;

    }

    private void setBfs(BitSet[][] arr, long node, int bfs){

        BitSet bitSet = ObjectBigArrays.get(arr,node);
        if(bitSet == null)
            bitSet = new BitSet(numSources);
        bitSet.set(bfs);
        ObjectBigArrays.set(arr,node,bitSet);
    }

    /**
     * Performs a multi-source breadth-first search in parallel
     * @throws InterruptedException If the time ran out
     */
    private void MSBFS() throws InterruptedException {

        visitNext = createBitsets();
        visitHadContent = new AtomicBoolean(true);

        long nodesPerProcessor = graph.getNumberOfNodes() / threads;
        iteration = 0;
        while(visitHadContent.get()){

            iterate(nodesPerProcessor);
            iteration++;

            if(visitHadContent.get()) {

                Traveler[][] temp = travelers;
                travelers = travelersNext;
                travelersNext = temp;

                visit = visitNext;
                visitNext = createBitsets();
            }
        }

    }

    private void iterate(long nodesPerProcessor) throws InterruptedException {
        visitHadContent.set(false);
        ArrayList<Future<?>> futures = new ArrayList<>(threads);
        threadsLeft = threads;

        for(long i = 0; i < threads; i++) {
            long start = i*nodesPerProcessor;
            long end = i == threads-1 ? graph.getNumberOfNodes() : start + nodesPerProcessor;
            futures.add(threadManager.submit(bothPhasesIterator(start,end, graph.getNodeIterator(start))));

        }
        awaitThreads(futures);
    }

    private void awaitThreads(ArrayList<Future<?>> futures) throws InterruptedException {

        int running = threads;
        boolean superBreak = false;
        while (true) {
            for (Future<?> future : futures) {
                try {
                    future.get(2, TimeUnit.SECONDS);
                    if(running-- == 0){
                        superBreak = true;
                    }
                } catch (InterruptedException e) {
                    if (!threadFailure) {
                        throw e;
                    }else {
                        superBreak = true;
                    }
                } catch (ExecutionException e) {

                    threadFailure = true;
                    for (Future<?> future2 : futures)
                        future2.cancel(true);

                    if (e.getCause() instanceof RuntimeException)
                        throw (RuntimeException) e.getCause();
                    else
                        throw new RuntimeException("Some of the breadth-first search threads threw an exception", e.getCause());
                } catch (TimeoutException e) {
                }
            }
            if(superBreak)
                break;
        }
    }

    /**
     * Used to synchronize all the threads in bothPhasesIterator.
     *
     * @return True if the thread should continue calculation.
     */
    private synchronized boolean synchronize() throws InterruptedException {
        if(threadFailure){
            this.notifyAll();
            return false;
        }else {
            if (--threadsLeft == 0)
                this.notifyAll();
            else {
                try {
                    this.wait();
                }catch (InterruptedException e){
                    if(!threadFailure)
                        throw e;
                }
            }
            return !threadFailure;
        }
    }

    /**
     * Returns a runnable that performs both stages of an iteration for nodes  from startNode inclusive to endNode exclusive.
     * The position of the nodeIterator should be at the startNode.
     * @param startNode
     * @param endNode
     * @param nodeIt
     * @return
     */
    private Runnable bothPhasesIterator(long startNode, long endNode, NodeIterator nodeIt){
        return () -> {
            try {
                firstPhaseIterator(startNode,endNode, nodeIt);
                if(synchronize() && visitHadContent.get())
                    secondPhaseIterator(startNode, endNode);
            }catch (InterruptedException e){
                if(!threadFailure)
                    throw new RuntimeException(e);
            }
        };
    }

    private void firstPhaseIterator(long startNode, long endNode, NodeIterator nodeIt){
        long prevNode = startNode;
        if(startNode < endNode)
            nodeIt.nextLong();
        for(long node = startNode; node < endNode; node++) {
            BitSet visitN = ObjectBigArrays.get(visit,node);
            if (visitN == null || visitN.cardinality() == 0) continue;

            if (visitor != null) {
                visitor.visit(node,visitN, ObjectBigArrays.get(seen,node),iteration, hasTraveler ? ObjectBigArrays.get(travelers,node) : null);
                if(visitN.cardinality() == 0) continue;
            }

            nodeIt.skip(node-prevNode);
            prevNode = node;

            visitHadContent.set(true);
            LazyLongIterator neighbors = nodeIt.successors();
            long degree = nodeIt.outdegree();
            long neighbor;
            BitSet visitNeigh;
            for (long d = 0; d < degree; d++) {
                neighbor = neighbors.nextLong();

                visitNeigh = ObjectBigArrays.get(visitNext,neighbor);
                boolean wasNull = visitNeigh == null;

                synchronized (wasNull ? this : ObjectBigArrays.get(visitNext,neighbor)) {
                    if(wasNull) {
                        visitNeigh = ObjectBigArrays.get(visitNext,neighbor);
                        if(visitNeigh == null)
                            visitNeigh = new BitSet(numSources);
                        else
                            wasNull = false;
                    }

                    if(hasTraveler) {
                        Traveler toSet = ObjectBigArrays.get(travelers,node);
                        if (visitNeigh.cardinality() != 0)
                            toSet = toSet.merge(ObjectBigArrays.get(travelersNext,neighbor),iteration+1);
                        ObjectBigArrays.set(travelersNext,neighbor,toSet);
                    }
                    visitNeigh.or(visitN);

                    if(wasNull) {
                        ObjectBigArrays.set(visitNext,neighbor,visitNeigh);
                    }
                }
            }

        }
    }

    private void secondPhaseIterator(long startNode, long endNode){
        BitSet visitN;
        for(long node = startNode; node < endNode ; node++) {
            visitN = ObjectBigArrays.get(visitNext,node);
            if(visitN == null || visitN.cardinality() == 0) continue;

            if(ObjectBigArrays.get(seen,node) == null)
                ObjectBigArrays.set(seen,node,new BitSet(numSources));
            visitN.andNot(ObjectBigArrays.get(seen,node));
            ObjectBigArrays.get(seen,node).or(visitN);
        }
    }

    /**
     * A visitor can be specified to the Multi-Source Breadth-first search which will be called at every visit
     */
    public interface Visitor{

        /**
         *
         *  Note that several visitors are very likely to be called in parallel and they should
         * be thread safe
         *
         * @param node The visited node
         * @param bfsVisits Which BFS's that visited the node in this iteration. If the bit of a BFS is cleared
         *                  that BFS will stop propagating.
         * @param seen All BFS's that have seen this node in any iteration. This should never be modified
         * @param depth The length from the source node.
         * @param traveler The traveler that reached this node.
         *
         */
        void visit(long node, BitSet bfsVisits, BitSet seen, int depth, Traveler traveler);

    }

    /**
     * A traveler can be used to propagate data along with the BFS
     */
    public interface Traveler{

        /**
         *
         * The merge function will very likely be called in parallel. But it will never be
         * called for the same node in parallel. As the MSBFS propagates its BFS's in bulk
         * the travelers has to merge when two travelers reach a node at the same time.
         *
         * Note that the new traveler should not have the same reference as either this
         * traveler or {@code mergeWith} as changes to these travelers would change
         * the travelers that are at other nodes as well.
         *
         * @param mergeWith The other traveler arriving at a given node at the same time.
         * @param depth The current depth of the BFS
         * @return The merged traveler
         */
        Traveler merge(Traveler mergeWith, int depth);

    }

    private static class MSBreadthFirstThreadFactory implements ThreadFactory {

        private int factoryID;
        private AtomicInteger threadID = new AtomicInteger(1);

        public MSBreadthFirstThreadFactory(int factoryID) {
            this.factoryID = factoryID;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r,"MSBreadthFirstPool-" + factoryID + "-thread-" + threadID.getAndIncrement());
        }
    }

}
