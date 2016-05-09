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
    protected long numNodes = 0, numArcs = 0;
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

        addEdges(edges);

    }

    public void addEdges(Edge[] edges){

        if (edges.length == 0)
            return;

        EdgesAdder edgesAdder = new EdgesAdder(edges);
        if (empty){
            edgesAdder.setEdges();
        }else{
            edgesAdder.addEdges();
        }

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
        long node = -1;
        long out = 0;

        public TraverseIterator(long from){
            if(from != 0)
                skip(from);
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
        private int previousNeighborPos = 0, previousNeighborsLength = 0;

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

        public void setEdges(){

            for (Edge e : edges){
                newNumNodes = Math.max(newNumNodes,e.to);
                if(e.from != prevNode){
                    switchSourceNode(e.from);
                }
                if(e.to != prevToNode){
                    addNeighbor(e.to);
                }
                edgesLeft--;
            }
            length = i;
            LongBigArrays.set(newNodes,outIndex,outDegree);
            newNumNodes = Math.max(edges[edges.length-1].from,newNumNodes)+1;
            empty = false;

        }

        public void addEdges() {

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
                newNumArcs += copyAndWritePosition(pos, length,i);
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
            if(previousNeighborPos < previousNeighborsLength && previousNeighbors[previousNeighborPos] == neighbor)
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
            newNodes = LongBigArrays.ensureCapacity(newNodes,i+edgesLeft+ previousNeighborsLength+HEADER_LENGTH);
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
                    copyEdgesUpToThisSource(sourceNode, curNode);
                }
                storePreviousNeighbors();
            }
        }

        /**
         * Stores all neighbors to the current node in {@code curNodes} that existed previously into previousNeighbors
         */
        private void storePreviousNeighbors() {
            if(!reachedEnd){
                if(previousNeighbors.length < curNodes.outdegree())
                    previousNeighbors = new long[Math.max(previousNeighbors.length*2,(int)curNodes.outdegree())];
                previousNeighborsLength = (int)curNodes.outdegree();
                LongBigArrays.copyFromBig(nodes,curNodes.getPosition()+HEADER_LENGTH, previousNeighbors,0,(int)curNodes.outdegree());
                previousNeighborPos = 0;
            }else
                previousNeighbors = new long[0];
        }

        /**
         * Copies all previous edges up to {@code sourceNode} and returns the new current node.
         * @param sourceNode
         * @param curNode
         */
        private void copyEdgesUpToThisSource(long sourceNode, long curNode) {
            long startPos = curNodes.getPosition();
            long copyEnd, skip = sourceNode-curNode;
            if(curNodes.skip(skip) < skip){
                copyEnd = length;
                reachedEnd = true;
            }else
                copyEnd = curNodes.getPosition();
            newNumArcs += copyAndWritePosition(startPos,copyEnd,i);
            i += copyEnd-startPos;
        }

        /**
         * Copies all the neighbors form {@code previousNeighbors} that are less than {@code neighbor} (or all if null).
         * and adds them to the new list
         *
         * @param neighbor
         */
        private void copyPreviousNeighborsLessThan(Long neighbor) {
            while(previousNeighborPos < previousNeighborsLength && (neighbor == null || previousNeighbors[previousNeighborPos] < neighbor)){
                outDegree++;
                newNumArcs++;
                LongBigArrays.set(newNodes,i++, previousNeighbors[previousNeighborPos++]);
            }
        }

        /**
         * Copies all edges from the old list starting at {@code startPos} to the new list at {@code toPos}
         *
         * @param startPos start position in {@code nodes}
         * @param copyEnd end position in {@code nodes}
         * @param toPos start position in {@code newNodes}
         * @return
         */
        private long copyAndWritePosition(long startPos, long copyEnd, long toPos) {

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