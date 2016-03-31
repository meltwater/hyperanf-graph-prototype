package se.meltwater;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectBigLists;
import se.meltwater.graph.IGraph;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * A class to perform Multi-source Breadth-first searches on graphs using the algorithm
 * developed in the paper: "The More the Merrier: Efficient Multi-Source Graph Traversal"
 */
public class MSBreadthFirst {

    private IGraph graph;
    private long[] bfsSources;
    private int numSources;
    private int threadsLeft;
    private Visitor visitor;
    private Traveler[][] travelers;
    private Traveler[][] travelersNext;
    private Traveler[] originalTravelers;
    private long waitTime;
    private TimeUnit waitTimeUnit;
    private final static int DEFAULT_WAIT_TIME = 1;
    private final static TimeUnit DEFAULT_WAIT_TIME_UNIT = TimeUnit.HOURS;
    private Throwable threadException = null;
    private boolean controlledInterrupt = false;

    private AtomicBoolean visitHadContent;
    private int threads;
    private BitSet[][] visit;
    private BitSet[][] seen;
    private BitSet[][] visitNext;
    private int iteration;
    private boolean hasTraveler;

    /**
     * Initialize a Breadth-first search in {@code graph} from the nodes in {@code bfsSources}
     * @param bfsSources The source nodes to start BFS's from. The index of a node is used as an identifier for
     *                   the BFS starting at that node.
     * @param graph
     */
    public MSBreadthFirst(long[] bfsSources, IGraph graph){
        this(bfsSources,graph,null);
    }

    /**
     * Initialize a Breadth-first search in {@code graph} from the nodes in {@code bfsSources}. At each new visit,
     * the passed visitor will be called.
     * @param bfsSources The source nodes to start BFS's from. The index of a node is used as an identifier for
     *                   the BFS starting at that node.
     * @param graph
     * @param visitor
     * @param maxWaitTime  The maximum time the algorithm should wait for the threads to finish (default 1 hour)
     * @param waitTimeUnit The units which {@code maxWaitTime} is expressed in
     */
    public MSBreadthFirst(long[] bfsSources, Traveler[] travelers, IGraph graph, Visitor visitor, long maxWaitTime, TimeUnit waitTimeUnit){

        this.graph = graph;
        numSources = bfsSources.length;
        this.bfsSources = bfsSources;
        this.visitor = visitor;
        this.waitTime = maxWaitTime;
        this.waitTimeUnit = waitTimeUnit;
        originalTravelers = travelers;
        hasTraveler = originalTravelers != null;
        if(travelers == null) {
            this.travelers = null;
            this.travelersNext = null;
        }else {
            this.travelers = ObjectBigArrays.newBigArray(new Traveler[0][0], graph.getNumberOfNodes());
            travelersNext = ObjectBigArrays.newBigArray(new Traveler[0][0], graph.getNumberOfNodes());
        }

    }

    /**
     * Initialize a Breadth-first search in {@code graph} from the nodes in {@code bfsSources}. At each new visit,
     * the passed visitor will be called.
     * @param bfsSources The source nodes to start BFS's from. The index of a node is used as an identifier for
     *                   the BFS starting at that node.
     * @param graph
     * @param visitor
     * */
     public MSBreadthFirst(long[] bfsSources, IGraph graph, Visitor visitor){
         this(bfsSources, null,graph,visitor);
     }

    /**
     * Initialize a Breadth-first search in {@code graph} from the nodes in {@code bfsSources}. At each new visit,
     * the passed visitor will be called.
     * @param bfsSources The source nodes to start BFS's from. The index of a node is used as an identifier for
     *                   the BFS starting at that node.
     * @param travelers
     * @param graph
     * @param visitor
     * */
    public MSBreadthFirst(long[] bfsSources, Traveler[] travelers, IGraph graph, Visitor visitor){
        this(bfsSources, travelers,graph,visitor,DEFAULT_WAIT_TIME,DEFAULT_WAIT_TIME_UNIT);
    }

    /**
     * Initialize a Breadth-first search in {@code graph} from the nodes in {@code bfsSources}
     * @param bfsSources The source nodes to start BFS's from. The index of a node is used as an identifier for
     *                   the BFS starting at that node.
     * @param graph
     * @param maxWaitTime  The maximum time the algorithm should wait for the threads to finish (default 1 hour)
     * @param waitTimeUnit The units which {@code maxWaitTime} is expressed in
     */
    public MSBreadthFirst(long[] bfsSources, IGraph graph, long maxWaitTime, TimeUnit waitTimeUnit){
        this(bfsSources, null,graph,null,maxWaitTime,waitTimeUnit);
    }

    private BitSet[][] createBitsets(){
        BitSet[][] list = ObjectBigArrays.newBigArray(new BitSet[0][0],graph.getNumberOfNodes());
        for(long node = 0; node < graph.getNumberOfNodes() ; node++)
            ObjectBigArrays.set(list,node,null);
        return list;
    }

    private void fillWithNewBitSets(BitSet[][] bsets){
        for(long node = 0; node < graph.getNumberOfNodes() ; node++)
            ObjectBigArrays.set(bsets,node,null);
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
    public BitSet[][] breadthFirstSearch() throws InterruptedException {

        threadException = null;
        controlledInterrupt = false;
        visit = createBitsets();
        seen = createBitsets();

        Traveler temp;
        for(int bfs = 0; bfs < numSources; bfs++){
            long node = bfsSources[bfs];
            setBfs(visit,node,bfs);
            setBfs(seen,node,bfs);
            if(hasTraveler) {
                temp = ObjectBigArrays.get(travelers,node);
                temp = temp == null ? originalTravelers[bfs] : temp.merge(originalTravelers[bfs],0);
                ObjectBigArrays.set(travelers,node,temp);
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
        threads = Runtime.getRuntime().availableProcessors();
        while(visitHadContent.get()){

            iterate();
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



    private void iterate() throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        long nodesPerProcessor = graph.getNumberOfNodes() / threads;
        visitHadContent.set(false);
        threadsLeft = threads;
        ArrayList<Future<?>> futures = new ArrayList<>(threads);

        for(long i = 0; i < threads; i++) {
            long start = i*nodesPerProcessor;
            long end = i == threads-1 ? graph.getNumberOfNodes() : start + nodesPerProcessor;
            futures.add(pool.submit(bothPhasesIterator(start,end, graph.getNodeIterator(start))));

        }
        pool.shutdown();
        boolean normalTermination = pool.awaitTermination(waitTime,waitTimeUnit);
        if(!normalTermination){
            controlledInterrupt = true;
            for(Future<?> future : futures)
                future.cancel(true);
            throw new InterruptedException("The time ran out");
        }
        if(threadException != null) {
            throw new RuntimeException("One of the breadth-first search threads threw an Exception",threadException);
        }
    }

    /**
     * Used to synchronize all the threads in bothPhasesIterator.
     * @param forceQuit If not null, Awakes all thread and tells them to terminate
     * @return True if the thread should continue calculation.
     */
    private synchronized boolean synchronize(Throwable forceQuit) {
        if(forceQuit != null){
            threadException = forceQuit;
            this.notifyAll();
            return false;
        }else {
            if (threadException != null)
                return false;
            if (--threadsLeft == 0)
                this.notifyAll();
            else {
                try {
                    this.wait();
                }catch (InterruptedException e){
                    if(!controlledInterrupt)
                        threadException = e;
                    return false;
                }
            }
            return threadException == null;
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
                firstPhaseIterator(endNode, nodeIt);
                if(synchronize(null) && visitHadContent.get())
                    secondPhaseIterator(startNode, endNode);
            }catch (Throwable e){
                synchronize(e);
            }
        };
    }

    private void firstPhaseIterator(long endNode, NodeIterator nodeIt){
        long node;
        while(nodeIt.hasNext() && (node = nodeIt.nextLong()) < endNode) {
            BitSet visitN = ObjectBigArrays.get(visit,node);
            if (visitN == null || visitN.cardinality() == 0) continue;

            if (visitor != null) {
                visitor.visit(node,visitN, ObjectBigArrays.get(seen,node),iteration, hasTraveler ? ObjectBigArrays.get(travelers,node) : null);
                if(visitN.cardinality() == 0) continue;
            }


            visitHadContent.set(true);
            LazyLongIterator neighbors = nodeIt.successors();
            long degree = nodeIt.outdegree();
            long neighbor;
            BitSet visitNeigh;
            for (long d = 0; d < degree; d++) {
                neighbor = neighbors.nextLong();
                if(ObjectBigArrays.get(visitNext,neighbor) == null)
                    ObjectBigArrays.set(visitNext,neighbor,new BitSet(numSources));
                synchronized (ObjectBigArrays.get(visitNext,neighbor)) {
                    visitNeigh = ObjectBigArrays.get(visitNext,neighbor);
                    if(hasTraveler) {
                        Traveler toSet = ObjectBigArrays.get(travelers,node);
                        if (visitNeigh.cardinality() != 0)
                            toSet = toSet.merge(ObjectBigArrays.get(travelersNext,neighbor),iteration+1);
                        ObjectBigArrays.set(travelersNext,neighbor,toSet);
                    }
                    visitNeigh.or(visitN);
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


}
