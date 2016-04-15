package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import java.util.Arrays;
import java.util.Comparator;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class TraverseGraph extends ImmutableGraph {

    protected long[][] nodes;
    protected Long2LongOpenHashMap nodePoss;
    protected long numNodes, numArcs;
    protected long length = 0;
    protected final static int HEADER_LENGTH = 2;
    protected boolean empty = false;

    public TraverseGraph(){
        empty = true;
    }

    public TraverseGraph(Edge[] edges){

        if(edges.length == 0) {
            empty = true;
            return;
        }

        init(edges);

    }

    public void init(Edge[] edges){

        empty = false;
        nodePoss = new Long2LongOpenHashMap();
        nodePoss.defaultReturnValue(-2);
        numArcs = 0;
        Arrays.sort(edges,edgeComparator());
        nodes = LongBigArrays.newBigArray(edges.length*3);
        long prevNode = -1, prevToNode = -1;
        long i = 0;
        long edgesLeft = edges.length;
        long outIndex = 0, outDegree = 0;
        for(Edge e : edges){
            numNodes = Math.max(numNodes,e.to);
            if(e.from != prevNode){
                prevNode = e.from;
                LongBigArrays.set(nodes,outIndex,outDegree);
                prevToNode = -1;
                outDegree = 0;
                nodes = LongBigArrays.ensureCapacity(nodes,i+edgesLeft+HEADER_LENGTH);
                nodePoss.put(e.from,i);
                LongBigArrays.set(nodes,i++,e.from);
                outIndex = i++;
            }
            if (e.to != prevToNode) {
                prevToNode = e.to;
                outDegree++;
                LongBigArrays.set(nodes, i++, e.to);
                numArcs++;
            }
            edgesLeft--;
        }
        length = i;
        LongBigArrays.set(nodes,outIndex,outDegree);
        numNodes = Math.max(edges[edges.length-1].from,numNodes) + 1;

    }

    public void addEdges(Edge[] edges){

        if (empty){
            init(edges);
            return;
        }

        EdgesAdder edgesAdder = new EdgesAdder(edges).invoke();

        numNodes = edgesAdder.getNewNumNodes();
        numArcs = edgesAdder.getNewNumArcs();
        nodes = edgesAdder.getNewNodes();
        nodePoss = edgesAdder.getNewNodePoss();


    }

    private Comparator<Edge> edgeComparator(){
        return (Edge e1,Edge e2) -> {
            if(e1.from > e2.from)
                return 1;
            if(e1.from < e2.from)
                return -1;
            if(e1.to > e2.to)
                return 1;
            if(e1.to < e2.to)
                return -1;
            return 0;
        };
    }

    @Override
    public long numNodes() {
        return numNodes;
    }

    @Override
    public boolean randomAccess() {
        return true;
    }

    @Override
    public long outdegree(long node) {
        long pos = nodePoss.get(node);
        if(pos == nodePoss.defaultReturnValue())
            return 0;
        return LongBigArrays.get(nodes,pos+1);
    }

    //TODO
    @Override
    public ImmutableGraph copy() {
        return null;
    }

    @Override
    public long numArcs() {
        return numArcs;
    }

    @Override
    public LazyLongIterator successors(long node){
        long pos = nodePoss.get(node);
        if(pos == nodePoss.defaultReturnValue())
            return LazyLongIterators.EMPTY_ITERATOR;
        return new LongBigArrayIterator(pos+HEADER_LENGTH,LongBigArrays.get(nodes,pos+1));
    }

    @Override
    public NodeIterator nodeIterator(long from) {
        return new TraverseIterator(from);
    }

    @Override
    public NodeIterator nodeIterator() {
        return nodeIterator(0);
    }

    private class LongBigArrayIterator implements LazyLongIterator{

        private long pos, left;

        public LongBigArrayIterator(long pos, long out){
            this.pos = pos;
            this.left = out;
        }

        @Override
        public long nextLong() {
            if(left-- <= 0)
                return -1;
            return LongBigArrays.get(nodes,pos++);
        }

        @Override
        public long skip(long l) {
            int i = 0;
            while( i < l && nextLong() != -1) i++;
            return i;
        }
    }

    private class TraverseIterator extends NodeIterator{

        long curPos = 0;
        long node = 0;
        long out = 0;

        public TraverseIterator(long from){
            node = from-1;
        }

        public long getPosition(){
            return curPos;
        }

        @Override
        public LazyLongIterator successors() {
            if(out == 0){
                return LazyLongIterators.EMPTY_ITERATOR;
            }
            return new LongBigArrayIterator(curPos+HEADER_LENGTH,out);
        }

        @Override
        public long nextLong() {
            node++;
            if(out > 0)
                curPos += out+HEADER_LENGTH;
            if(curPos >= LongBigArrays.length(nodes) || LongBigArrays.get(nodes,curPos) != node)
                out = 0;
            else
                out = LongBigArrays.get(nodes,curPos+1);
            return node;
        }

        @Override
        public long outdegree() {
            return out;
        }

        @Override
        public boolean hasNext() {
            return node + 1 < numNodes;
        }

    }

    private class EdgesAdder {
        private Edge[] edges;
        private Long2LongOpenHashMap newNodePoss;
        private long newNumNodes;
        private long newNumArcs;
        private long[][] newNodes;
        private TraverseIterator curNodes = new TraverseIterator(0);
        private long[] previousNeighbors = new long[0];
        private int previousNeighborPos = 0;

        private long prevNode = -1, prevToNode = -1;
        private long i = 0;
        private long edgesLeft;
        private long outIndex = 0, outDegree = 0;
        private boolean reachedEnd = false, first = true;

        public EdgesAdder(Edge[] edges) {
            this.edges = edges;
            this.newNodePoss = new Long2LongOpenHashMap();
            newNodePoss.defaultReturnValue(-2);
            this.newNumNodes = numNodes-1;
            this.newNumArcs = 0;
            Arrays.sort(edges,edgeComparator());
            newNodes = LongBigArrays.newBigArray(numArcs + numNodes*2 + edges.length*3);
            edgesLeft = edges.length;
        }

        public long[][] getNewNodes() {
            return newNodes;
        }

        public long getNewNumNodes() {
            return newNumNodes;
        }

        public long getNewNumArcs() {
            return newNumArcs;
        }

        public Long2LongOpenHashMap getNewNodePoss(){
            return newNodePoss;
        }

        public EdgesAdder invoke() {

            for(Edge e : edges){
                newNumNodes = Math.max(newNumNodes,e.to);
                if(e.from != prevNode){
                    // If we switched source node the previous source node may not have all its previous neighbors
                    // copied so we copy the rest of them
                    copyPreviousNeighborsLessThan(null);
                    handlePreviousEdgesUpTo(e.from);
                    switchSourceNode(e.from);
                }
                if (e.to != prevToNode) {
                    copyPreviousNeighborsLessThan(e.to);
                    addNeighbor(e.to);
                }
                edgesLeft--;
            }
            // If we went through all edges the last source node may not have all its previous neighbors
            // copied so we copy the rest of them
            copyPreviousNeighborsLessThan(null);
            copyRemainingPreviousArcs();
            length = i;
            LongBigArrays.set(newNodes,outIndex,outDegree);
            newNumNodes = Math.max(edges[edges.length-1].from,newNumNodes) + 1;
            return this;
        }

        /**
         * Copies all remaining nodes from the previous nodes list to the new one. Also writes the position
         * of the nodes to the new node positions.
         */
        private void copyRemainingPreviousArcs() {
            if(curNodes.hasNext()){
                curNodes.nextLong();
                long pos = curNodes.getPosition();
                newNodes = LongBigArrays.ensureCapacity(newNodes,i+(length-pos));
                newNumArcs += copyAndWritePosition(pos, length,newNodes,i,newNodePoss);
                i += length-pos;
            }
        }

        /**
         * Increases number of arcs and out degree. If the top node in the previous neighbors are the same as
         * {@code neighbor} the top is moved forward.
         *
         * @param neighbor The neighbor to add
         */
        private void addNeighbor(long neighbor) {
            if(previousNeighborPos < previousNeighbors.length && previousNeighbors[previousNeighborPos] == neighbor)
                previousNeighborPos++;
            prevToNode = neighbor;
            outDegree++;
            LongBigArrays.set(newNodes, i++, neighbor);
            newNumArcs++;
        }

        /**
         * Writes the previous outdegree to the specified location (except for the first time). Resets all
         * source specific members. Writes the source node and its position. Reserves index for outdegree.
         *
         * @param newSource The source to switch to
         */
        private void switchSourceNode(long newSource) {
            prevNode = newSource;
            if(first == (first = false))
                LongBigArrays.set(newNodes,outIndex,outDegree);
            prevToNode = -1;
            outDegree = 0;
            newNodes = LongBigArrays.ensureCapacity(newNodes,i+edgesLeft+ previousNeighbors.length+HEADER_LENGTH);
            newNodePoss.put(newSource,i);
            LongBigArrays.set(newNodes,i++,newSource);
            outIndex = i++;
        }

        /**
         * If the source node of the edge is higher than the current node (which is one more than the previous source)
         * all previous arcs between are copied. If there were neighbors to the source previously, these are stored
         * in {@code previousNeighbors}
         * @param sourceNode
         */
        private void handlePreviousEdgesUpTo(long sourceNode) {
            if(curNodes.hasNext()) {
                long curNode = curNodes.nextLong();
                if(curNode < sourceNode){
                    curNode = copyEdgesUpToThisSource(sourceNode, curNode);
                }
                storePreviousNeighbors(sourceNode, curNode);
            }
        }

        /**
         * Stores all neigbhbors to {@code sourceNode} that existed previously
         *
         * @param sourceNode
         * @param curNode
         */
        private void storePreviousNeighbors(long sourceNode, long curNode) {
            if(curNode == sourceNode && !reachedEnd){
                previousNeighbors = new long[(int)curNodes.outdegree()];
                LongBigArrays.copyFromBig(nodes,curNodes.getPosition()+HEADER_LENGTH, previousNeighbors,0,(int)curNodes.outdegree());
                previousNeighborPos = 0;
            }else
                previousNeighbors = new long[0];
        }

        /**
         * Copies all edges TODO
         * @param sourceNode
         * @param curNode
         * @return
         */
        private long copyEdgesUpToThisSource(long sourceNode, long curNode) {
            long startPos = curNodes.getPosition();
            long copyEnd, skip = sourceNode-curNode;
            if(curNodes.skip(skip) < skip){
                copyEnd = length;
                reachedEnd = true;
            }else
                copyEnd = curNodes.getPosition();
            newNumArcs += copyAndWritePosition(startPos,copyEnd,newNodes,i,newNodePoss);
            i += copyEnd-startPos;
            curNode += skip;
            return curNode;
        }

        private void copyPreviousNeighborsLessThan(Long neighbor) {
            while(previousNeighborPos < previousNeighbors.length && (neighbor == null || previousNeighbors[previousNeighborPos] < neighbor)){
                outDegree++;
                newNumArcs++;
                LongBigArrays.set(newNodes,i++, previousNeighbors[previousNeighborPos++]);
            }
        }

        private long copyAndWritePosition(long startPos, long copyEnd, long[][] newNodes, long toPos, Long2LongOpenHashMap newNodePoss) {

            long curPos = startPos;
            long posDiff = toPos-startPos;
            long arcsAdded = 0;
            while(curPos < copyEnd && curPos < length){
                long node = LongBigArrays.get(nodes,curPos);
                long out = LongBigArrays.get(nodes,curPos+1);
                arcsAdded += out;
                newNodePoss.put(node,curPos+posDiff);
                LongBigArrays.copy(nodes,curPos,newNodes,curPos+posDiff,out+HEADER_LENGTH);
                curPos += out+HEADER_LENGTH;
            }
            return arcsAdded;

        }
    }
}