package se.meltwater.graphEditing;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.*;
import java.util.Properties;


/**
 * Created by simon on 2016-02-16.
 */
public class GraphMerger{

    protected FlaggedInputBitStream graph1Stream;
    protected FlaggedInputBitStream graph2Stream;

    protected OutputBitStream graphOut;
    protected OutputBitStream offsetsOut;

    protected String graph1, graph2, newGraph;


    protected GraphMerger(String graph1, String graph2, String newBasename) {
        this.graph1 = graph1;
        this.graph2 = graph2;
        this.newGraph = newBasename;
    }

    /**
     * Merges two bvgraphs (graph1 and graph2) to a new one (newBasename). This method was mainly developed to focus on
     * speed and as copy-blocks require a lot of extra calculation to be optimized they are not supported by this method.
     * @param graph1
     * @param graph2
     * @param newBasename
     * @throws IOException If an error occurred whe reading the graph files.
     */
    public static void mergeGraphs(String graph1, String graph2, String newBasename) throws IOException {
        new GraphMerger(graph1, graph2, newBasename).merge();
    }

    protected void merge() throws IOException {

        GraphProperties gp1, gp2;

        try {

            gp1 = new GraphProperties(graph1 + ".properties");
            gp2 = new GraphProperties(graph2 + ".properties");
            if(gp1.getWindowsize() != 0 || gp2.getWindowsize() != 0)
                throw new IllegalArgumentException("Can only merge graphs without blocks.");

            graph1Stream = new FlaggedInputBitStream(new FileInputStream(graph1 + ".graph"), gp1.getCompressionFlags(),gp1.getZetak());
            graph2Stream = new FlaggedInputBitStream(new FileInputStream(graph2 + ".graph"), gp2.getCompressionFlags(),gp2.getZetak());

        }catch(FileNotFoundException e){
            System.err.println("Couldn't find graph file");
            e.printStackTrace();
            return;
        }

        graphOut = new OutputBitStream(new FileOutputStream(newGraph + ".graph"));
        offsetsOut = new OutputBitStream(new FileOutputStream(newGraph + ".offsets"));

        nodes = Math.max(gp1.getNumNodes(),gp2.getNumNodes());

        long prevWrittenBits = 0;

        for (long node = 0; node < nodes; node++) {

            offsetsOut.writeLongGamma(graphOut.writtenBits() - prevWrittenBits);
            prevWrittenBits = graphOut.writtenBits();

            int out1 = node < gp1.getNumNodes() ? graph1Stream.readOutdegree() : 0;
            int out2 = node < gp2.getNumNodes() ? graph2Stream.readOutdegree() : 0;
            if (out1 == 0 && out2 == 0) {
                graphOut.writeGamma(0);
                continue;
            }
            int nodesLeft1 = out1, nodesLeft2 = out2;

            int intervalCount1 = nodesLeft1 > 0 ? graph1Stream.readGamma() : 0;
            int intervalCount2 = nodesLeft2 > 0 ? graph2Stream.readGamma() : 0;
            long[][] intervals1 = new long[intervalCount1][2], intervals2 = new long[intervalCount2][2];
            nodesLeft1 -= readIntervals(intervalCount1, intervals1, node, graph1Stream, gp1.getIntervalLength());
            nodesLeft2 -= readIntervals(intervalCount2, intervals2, node, graph2Stream, gp2.getIntervalLength());
            long[][] resultingIntervals = new long[intervalCount1 + intervalCount2][2];
            Pair<Integer, Integer> result = mergeIntervals(intervals1, intervals2, resultingIntervals);
            int numIntervals = result.fst, numNodesInIntervals = result.snd;
            intervalisedarcs += numNodesInIntervals;

            long[] residuals = new long[nodesLeft1 + nodesLeft2];
            int numResiduals = readResiduals(nodesLeft1, nodesLeft2, node, residuals, resultingIntervals);
            residualarcs += numResiduals;

            arcs += numResiduals+numNodesInIntervals;
            graphOut.writeGamma(numResiduals + numNodesInIntervals); //outdegree

            writeIntervalSection(numIntervals,resultingIntervals,node);
            writeResidualSection(numResiduals,residuals,node);

        }

        offsetsOut.writeLongGamma(graphOut.writtenBits()- prevWrittenBits);
        graphOut.close();
        offsetsOut.close();
        graph1Stream.close();
        graph2Stream.close();

        writeProperties();

    }

    private void writeProperties() throws IOException {

        Properties prop = new Properties();
        prop.setProperty("nodes",nodes + "");
        prop.setProperty("arcs",arcs + "");
        prop.setProperty("version",version + "");
        prop.setProperty("compressionflags", compressionflags);
        prop.setProperty("zetak",zetak + "");
        prop.setProperty("residualarcs",residualarcs + "");
        prop.setProperty("windowsize",windowsize + "");
        prop.setProperty("copiedarcs",copiedarcs + "");
        prop.setProperty("maxrefcount",maxrefcount + "");
        prop.setProperty("graphclass", graphclass + "");
        prop.setProperty("minintervallength",minintervallength + "");
        FileOutputStream propOut = new FileOutputStream(new File(newGraph + ".properties"));
        prop.store(propOut,"BVGraph properties");
        propOut.close();

    }

    private void writeIntervalSection(int numIntervals, long[][] intervals, long node) throws IOException {
        graphOut.writeGamma(numIntervals);
        for (int i = 0; i < numIntervals; i++) {
            if (i == 0)
                graphOut.writeLongGamma(Fast.int2nat(intervals[i][0] - node));
            else
                graphOut.writeLongGamma(intervals[i][0] - intervals[i - 1][1] - 1);
            graphOut.writeLongGamma(intervals[i][1] - intervals[i][0] - minintervallength);
        }
    }

    private  void writeResidualSection(int numResiduals, long[] residuals, long node) throws IOException {

        for (int i = 0; i < numResiduals; i++) {
            if (i == 0)
                graphOut.writeLongZeta(Fast.int2nat(residuals[i] - node), zetak);
            else
                graphOut.writeLongZeta(residuals[i] - residuals[i - 1] - 1, zetak);
        }

    }

    /**
     * Reads the next node from a graph ignoring nodes included in {@code intervalizedNodes}
     * @param g
     * @param nodesLeft
     * @param nodesRead
     * @param node
     * @param intervalizedNodes
     * @param prevNode
     * @return The fst contains the next node, the second contains the new nodesRead. If there are no next nodes,
     * fst will be Long.MAX_VALUE
     * @throws IOException
     */
    private static Pair<Long,Integer> readNextUnused(FlaggedInputBitStream g, int nodesLeft, int nodesRead, long node,
                                                     long[][] intervalizedNodes, long prevNode) throws IOException {
        long nextUnused = prevNode;
        if(nodesRead < nodesLeft) {
            nextUnused = nodesRead == 0 ? Fast.nat2int(g.readResidual()) + node : g.readResidual() + nextUnused + 1;
            while (isInIntervals(nextUnused, intervalizedNodes) && ++nodesRead < nodesLeft) {
                nextUnused = g.readResidual() + nextUnused + 1;
            }
        }
        if(nodesRead >= nodesLeft) //If there are no more nodes from graph one we should always take from the other graph
            nextUnused = Long.MAX_VALUE;
        return new Pair<>(nextUnused,nodesRead);
    }

    /**
     * Reads the residuals from both graphs at the same time. It will remove duplicates and nodes which is already
     * included in {@code intervalizedNodes}
     *
     * @param nodesLeft1
     * @param nodesLeft2
     * @param node
     * @param residuals array to place the residuals in
     * @param intervalizedNodes
     * @return Number of node
     * @throws IOException
     */
    private int readResiduals(int nodesLeft1, int nodesLeft2, long node, long[] residuals,
                                     long[][] intervalizedNodes) throws IOException {
        long n1 = Long.MAX_VALUE, n2 = Long.MAX_VALUE;
        int nodesRead1 = 0, nodesRead2 = 0;
        int i = 0;
        boolean used1 = true, used2 = true;
        while(nodesRead1 < nodesLeft1 || nodesRead2 < nodesLeft2){
            if(used1) { //the current node was used, read new node
                used1 = false;
                Pair<Long,Integer> res = readNextUnused(graph1Stream,nodesLeft1,nodesRead1,node,intervalizedNodes,n1);
                n1 = res.fst; nodesRead1 = res.snd;
            }
            if(used2) {
                used2 = false;
                Pair<Long,Integer> res = readNextUnused(graph2Stream,nodesLeft2,nodesRead2,node,intervalizedNodes,n2);
                n2 = res.fst; nodesRead2 = res.snd;
            }

            if(n1 == n2){
                if(n1 == Long.MAX_VALUE)
                    break;
                residuals[i++] = n1;
                nodesRead1++;
                nodesRead2++;
                used1 = used2 = true;
            }else if(n1 < n2){
                residuals[i++] = n1;
                nodesRead1++;
                used1 = true;
            }else{ //n2 < n1
                residuals[i++] = n2;
                nodesRead2++;
                used2 = true;
            }
        }
        return i;
    }

    /**
     * Checks if the value is in any of the specified intervals. The intervals has to be sorted by the end value.
     * @param value
     * @param intervals
     * @return
     */
    protected static boolean isInIntervals(long value, long[][] intervals){
        int i = 0;
        while(i < intervals.length && intervals[i][1] < value) i++;
        return i < intervals.length && intervals[i][0] <= value && intervals[i][1] > value;
    }

    /**
     * Checks if two intervals overlap or are right next to each other so that they can be joint into one.
     * @param interval1
     * @param interval2
     * @return
     */
    protected static boolean intervalsCanBeJoint(long[] interval1, long[] interval2){
        return interval1[0] <= interval2[1] && interval1[1] >= interval2[0];
    }

    /**
     * Tries to join two intervals to one. If possible, the new interval will be placed in {@code intervalToJoinTo}.
     * @param intervalToJoinTo
     * @param intervalToJoinFrom
     * @return If the intervals could and were joined.
     */
    protected static boolean tryToJoinIntervals(long[] intervalToJoinTo, long[] intervalToJoinFrom){
        if(intervalsCanBeJoint(intervalToJoinTo,intervalToJoinFrom)){
            intervalToJoinTo[0] = Math.min(intervalToJoinTo[0],intervalToJoinFrom[0]);
            intervalToJoinTo[1] = Math.max(intervalToJoinTo[1],intervalToJoinFrom[1]);
            return true;
        }
        return false;
    }

    /**
     * Merges two lists of intervals into one and tries to merge as many as possible. The resulting intervals are
     * placed in resultingIntervals.
     *
     * @param intervals1
     * @param intervals2
     * @param resultingIntervals
     * @return First is the number of intervals and second is the number of nodes included in the intervals
     */
    public static Pair<Integer,Integer> mergeIntervals(long[][] intervals1, long[][] intervals2, long[][] resultingIntervals){
        int intervalCount1 = intervals1.length, intervalCount2 = intervals2.length;
        int i=0,j=0,k=0;
        boolean foundInIndex = false;
        int totalNodes = 0;
        while(i < intervalCount1 || k < intervalCount2){
            boolean found = false;
            if(i < intervalCount1 && tryToJoinIntervals(resultingIntervals[j],intervals1[i])) {
                i++;
                found = foundInIndex = true;
            }
            if(k < intervalCount2 && tryToJoinIntervals(resultingIntervals[j],intervals2[k])) {
                k++;
                found = true;
                foundInIndex = true;
            }
            if(!found){
                if(foundInIndex) //If we are about to create a new interval we first save the number of nodes in the interval
                    totalNodes += resultingIntervals[j][1]-resultingIntervals[j][0];
                j = foundInIndex ? j+1 : j;
                foundInIndex = true;
                if(i < intervalCount1 && (k >= intervalCount2 || intervals1[i][0] < intervals2[k][0]))
                    resultingIntervals[j] = intervals1[i++];
                else
                    resultingIntervals[j] = intervals2[k++];
            }
        }
        if(foundInIndex)
            totalNodes += resultingIntervals[j][1]-resultingIntervals[j][0];
        return new Pair<>(foundInIndex ? j+1 : j,totalNodes);
    }

    private static int readIntervals(int length, long[][] readInto, long node, InputBitStream ibs, int minIntervalLength) throws IOException {
        int nodesRead = 0;
        for(int i = 0; i < length; i++) {
            if(i == 0)
                readInto[i][0] = Fast.nat2int(ibs.readLongGamma()) + node;
            else
                readInto[i][0] = ibs.readLongGamma() + readInto[i-1][1] + 1;

            int len = (int)ibs.readLongGamma()+minIntervalLength;
            nodesRead += len;
            readInto[i][1] = readInto[i][0]+len;
        }
        return nodesRead;
    }

    public static class Pair<P,Q>{
        public P fst;
        public Q snd;
        public Pair(P fst, Q snd){
            this.fst = fst;
            this.snd = snd;
        }
        public String toString(){
            return "(" + fst.toString() + "," + snd.toString() + ")";
        }
    }


    //double compratio = 2.288;
    //final int bitsforblocks=0;
    private int residualarcs=0;
    private final int version=0;
    private final int zetak = 3;
    //residualexpstats=2,0,2 not required
    //final int avgref=0;
    //residualavggap=3.250
    //avgbitsforoutdegrees=3.4
    private final int windowsize=0;
    //bitsforintervals=10
    private final int copiedarcs=0;
    //avgbitsforblocks=0
    //bitsperlink=5.333
    //bitsforresiduals=17
    //bitsforreferences=0
    //avgdist=0
    //successoravgloggap=1.5260292947924734
    //avgbitsforreferences=0
    //successoravggap=2.312
    private final int maxrefcount=0;
    //successorexpstats=5,1,2
    private long nodes;
    private String compressionflags = "";
    private int intervalisedarcs=0;
    private int arcs=0;
    // bitsforoutdegrees=17
    //avgbitsforintervals=2
    private int minintervallength=4;
    private String graphclass = "it.unimi.dsi.webgraph.BVGraph";
    //avgbitsforresiduals=3.4
    //residualavgloggap=1.850219859070546

}
