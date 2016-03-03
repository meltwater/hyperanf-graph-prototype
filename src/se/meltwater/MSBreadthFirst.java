package se.meltwater;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import se.meltwater.graph.IGraph;

import java.util.BitSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A class to perform Multi-source Breadth-first searches on graphs using the algorithm
 * developed in the paper: "The More the Merrier: Efficient Multi-Source Graph Traversal"
 */
public class MSBreadthFirst {

    private IGraph graph;
    private int[] bfsSources;
    private int numSources;
    private int threadsLeft;
    private Visitor visitor;

    /**
     * Initialize a Breadth-first search in {@code graph} from the nodes in {@code bfsSources}
     * @param bfsSources The source nodes to start BFS's from. The index of a node is used as an identifier for
     *                   the BFS starting at that node.
     * @param graph
     */
    public MSBreadthFirst(int[] bfsSources, IGraph graph){
        this(bfsSources,graph,null);
    }

    /**
     * Initialize a Breadth-first search in {@code graph} from the nodes in {@code bfsSources}. At each new visit,
     * the passed visitor will be called.
     * @param bfsSources The source nodes to start BFS's from. The index of a node is used as an identifier for
     *                   the BFS starting at that node.
     * @param graph
     * @param visitor
     */
    public MSBreadthFirst(int[] bfsSources, IGraph graph, Visitor visitor){

        this.graph = graph;
        numSources = bfsSources.length;
        this.bfsSources = bfsSources;
        this.visitor = visitor;

    }

    private BitSet[] createBitsets(){
        BitSet[] list = new BitSet[(int)graph.getNumberOfNodes()];
        for(int node = 0; node < graph.getNumberOfNodes() ; node++)
            list[node] = new BitSet(bfsSources.length);
        return list;
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
    public BitSet[] breadthFirstSearch() throws InterruptedException {

        BitSet[] visit = createBitsets(), seen = createBitsets();

        for(int bfs = 0; bfs < numSources; bfs++){
            visit[bfsSources[bfs]].set(bfs);
            seen[bfsSources[bfs]].set(bfs);
        }

        System.out.println("Starting Breadth first");
        long time = System.currentTimeMillis();

        MSBFS(visit,seen);
        System.out.println("Finished Breadth first in: " + (System.currentTimeMillis() - time) + "ms");

        return seen;

    }

    /**
     * Performs a multi-source breadth-first search in parallel
     * @param visit
     * @param seen
     * @throws InterruptedException
     */
    private void MSBFS(BitSet[] visit, BitSet[] seen) throws InterruptedException {

        BitSet[] visitNext = createBitsets();
        BoolWrapper visitHadContent = new BoolWrapper(true);
        int processors = Runtime.getRuntime().availableProcessors();
        int iteration = 0;
        while(visitHadContent.theBool){

            iterate(visitHadContent,processors,visit,seen,visitNext,iteration++);

            if(visitHadContent.theBool) {

                visit = visitNext;
                visitNext = createBitsets();
            }
            System.out.println("Finished iteration.");
        }

    }

    private void iterate(BoolWrapper visitHadContent, int threads,BitSet[] visit, BitSet[] seen,BitSet[] visitNext,
                         int iteration) throws InterruptedException {

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        int nodesPerProcessor = (int)graph.getNumberOfNodes() / threads;
        visitHadContent.theBool = false;
        threadsLeft = threads;

        for(int i = 0; i < threads; i++) {
            int start = i*nodesPerProcessor;
            int end = i == threads-1 ? (int)graph.getNumberOfNodes() : start + nodesPerProcessor;
            pool.submit(bothPhasesIterator(start,end,visit,visitNext,seen,visitHadContent, graph.getNodeIterator(start),iteration));
        }
        pool.shutdown();
        pool.awaitTermination(10,TimeUnit.MINUTES);
    }

    private synchronized void synchronize() throws InterruptedException {
        if(--threadsLeft == 0)
            this.notifyAll();
        else{
            this.wait();
        }
    }

    /**
     * Returns a runnable that performs both stages of an iteration for nodes  from startNode inclusive to endNode exclusive.
     * The position of the nodeIterator should be at the startNode.
     * @param startNode
     * @param endNode
     * @param visit
     * @param visitNext
     * @param seen
     * @param visitHadContent
     * @param nodeIt
     * @param iteration
     * @return
     */
    private Runnable bothPhasesIterator(int startNode, int endNode, BitSet[] visit, BitSet[] visitNext,
                                        BitSet[] seen, BoolWrapper visitHadContent, NodeIterator nodeIt, int iteration){
        return () -> {
            try {
                firstPhaseIterator(endNode, visit, visitNext, seen, visitHadContent, nodeIt, iteration);
                synchronize();
                if(visitHadContent.theBool)
                    secondPhaseIterator(startNode, endNode, visitNext, seen);
            }catch (InterruptedException e){
                System.err.println("Thread was interrupted");
                e.printStackTrace();

            }
        };
    }

    private void firstPhaseIterator(int endNode, BitSet[] visit, BitSet[] visitNext, BitSet[] seen,
                                        BoolWrapper visitHadContent, NodeIterator nodeIt, int iteration){
        int node;
        while(nodeIt.hasNext() && (node = (int)nodeIt.nextLong()) < endNode) {
            if (visit[node].cardinality() == 0) continue;

            if (visitor != null) {
                visitor.visit(node,visit[node], seen[node],iteration);
            }

            if(visit[node].cardinality() == 0) continue;

            visitHadContent.theBool = true;
            LazyLongIterator neighbors = nodeIt.successors();
            long degree = nodeIt.outdegree();
            int neighbor;
            for (long d = 0; d < degree; d++) {
                neighbor = (int)neighbors.nextLong();
                synchronized (visitNext[neighbor]) {
                    visitNext[neighbor].or(visit[node]);
                }
            }

        }
    }

    private void secondPhaseIterator(int startNode, int endNode, BitSet[] visitNext, BitSet[] seen){
        for(int node = startNode; node < endNode ; node++) {
            if(visitNext[node].cardinality() == 0) continue;

            visitNext[node].andNot(seen[node]);
            seen[node].or(visitNext[node]);
        }
    }

    class BoolWrapper{
        public boolean theBool = false;
        public BoolWrapper(boolean initialValue){ theBool = initialValue; }
        public BoolWrapper(){this(false);}
    }

    /**
     * A visitor can be specified to the Multi-Source Breadth-first search which will be called at every visit
     */
    public interface Visitor{
        /**
         * The BitSet parameter will contain the bfs's that reach the node in this iteration.
         * The BitSet will never contain the bfs's that have visited the node in an earlier iteration
         * If any of the bfs's should stop searching at the current node and iteration it can be
         * achieved by clearing the bit for that bfs in the BitSet
         *
         * @apiNote Note that several visitors are very likely to be called in parallel and they should
         * be thread safe
         *
         * @param node The visited node
         * @param bfsVisits Which BFS's that visited the node in this iteration. If the bit of a BFS is cleared
         *                  that BFS will stop propagating.
         * @param seen All BFS's that have seen this node in any iteration.
         * @param depth The length from the source node.
         *
         */
        void visit(long node, BitSet bfsVisits, BitSet seen, int depth);

    }


}
