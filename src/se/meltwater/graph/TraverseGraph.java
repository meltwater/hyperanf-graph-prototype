package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterators;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.compression.TreeDecoder;
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

        long[][] newNodes;
        Long2LongOpenHashMap newNodePoss = new Long2LongOpenHashMap();
        newNodePoss.defaultReturnValue(-2);
        long newNumNodes = numNodes-1, newNumArcs = 0;

        TraverseIterator curNodes = new TraverseIterator(0);
        long[] extraNodes = new long[0];
        int extraNodePos = 0;

        Arrays.sort(edges,edgeComparator());
        newNodes = LongBigArrays.newBigArray(numArcs + numNodes*2 + edges.length*3);
        long prevNode = -1, prevToNode = -1;
        long i = 0;
        long edgesLeft = edges.length;
        long outIndex = 0, outDegree = 0;
        boolean reachedEnd = false, first = true;
        for(Edge e : edges){
            newNumNodes = Math.max(newNumNodes,e.to);
            if(e.from != prevNode){
                prevNode = e.from;
                while(extraNodePos < extraNodes.length){
                    outDegree++;
                    newNumArcs++;
                    LongBigArrays.set(newNodes,i++,extraNodes[extraNodePos++]);
                }
                if(curNodes.hasNext()) {
                    long curNode;
                    curNode = curNodes.nextLong();
                    if(curNode < e.from){
                        long startPos = curNodes.getPosition();
                        long copyEnd, skip = e.from-curNode;
                        if(curNodes.skip(skip) < skip){
                            copyEnd = length;
                            reachedEnd = true;
                        }else
                            copyEnd = curNodes.getPosition();
                        newNumArcs += copyAndWritePosition(startPos,copyEnd,newNodes,i,newNodePoss);
                        i += copyEnd-startPos;
                        curNode += skip;
                    }
                    if(curNode == e.from && !reachedEnd){
                        extraNodes = new long[(int)curNodes.outdegree()];
                        LongBigArrays.copyFromBig(nodes,curNodes.getPosition()+HEADER_LENGTH,extraNodes,0,(int)curNodes.outdegree());
                        extraNodePos = 0;
                    }else
                        extraNodes = new long[0];
                }
                if(first == (first = false))
                    LongBigArrays.set(newNodes,outIndex,outDegree);
                prevToNode = -1;
                outDegree = 0;
                newNodes = LongBigArrays.ensureCapacity(newNodes,i+edgesLeft+extraNodes.length+HEADER_LENGTH);
                newNodePoss.put(e.from,i);
                LongBigArrays.set(newNodes,i++,e.from);
                outIndex = i++;
            }
            if (e.to != prevToNode) {
                while(extraNodePos < extraNodes.length && extraNodes[extraNodePos] < e.to){
                    outDegree++;
                    newNumArcs++;
                    LongBigArrays.set(newNodes,i++,extraNodes[extraNodePos++]);
                }
                if(extraNodePos < extraNodes.length && extraNodes[extraNodePos] == e.to)
                    extraNodePos++;
                prevToNode = e.to;
                outDegree++;
                LongBigArrays.set(newNodes, i++, e.to);
                newNumArcs++;
            }
            edgesLeft--;
        }
        while(extraNodePos < extraNodes.length){
            outDegree++;
            newNumArcs++;
            LongBigArrays.set(newNodes,i++,extraNodes[extraNodePos++]);
        }
        if(curNodes.hasNext()){
            curNodes.nextLong();
            long pos = curNodes.getPosition();
            newNodes = LongBigArrays.ensureCapacity(newNodes,i+(length-pos));
            newNumArcs += copyAndWritePosition(pos, length,newNodes,i,newNodePoss);
            i += length-pos;
        }
        length = i;
        LongBigArrays.set(newNodes,outIndex,outDegree);
        numNodes = Math.max(edges[edges.length-1].from,newNumNodes) + 1;
        numArcs = newNumArcs;
        nodes = newNodes;
        nodePoss = newNodePoss;


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

}