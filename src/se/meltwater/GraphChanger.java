package se.meltwater;

import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.big.webgraph.BVGraph;

import java.io.*;
import java.util.Properties;


/**
 * Created by simon on 2016-02-16.
 */
public class GraphChanger extends BVGraph{

    private OutputBitStream graph;
    private OutputBitStream offsetStream;
    private int[] offsetOffsets; // The offset to an offset for a given node

    public GraphChanger() {
    }

    public static void merge(String g1, String g2, String newBasename) throws IOException {

        //double compratio = 2.288;
        //final int bitsforblocks=0;
        int residualarcs=0;
        final int version=0;
        final int zetak = 3;
        //residualexpstats=2,0,2 not required
        //final int avgref=0;
        //residualavggap=3.250
        //avgbitsforoutdegrees=3.4
        final int windowsize=0;
        //bitsforintervals=10
        final int copiedarcs=0;
        //avgbitsforblocks=0
        //bitsperlink=5.333
        //bitsforresiduals=17
        //bitsforreferences=0
        //avgdist=0
        //successoravgloggap=1.5260292947924734
        //avgbitsforreferences=0
        //successoravggap=2.312
        final int maxrefcount=0;
        //successorexpstats=5,1,2
        long nodes;
        String compressionflags = "";
        int intervalisedarcs=0;
        int arcs=0;
        // bitsforoutdegrees=17
        //avgbitsforintervals=2
        int minintervallength=4;
        String graphclass = "it.unimi.dsi.webgraph.BVGraph";
        //avgbitsforresiduals=3.4
        //residualavgloggap=1.850219859070546

        InputBitStream graph1Stream = new InputBitStream(new FileInputStream(g1 + ".graph"));
        InputBitStream graph2Stream = new InputBitStream(new FileInputStream(g2 + ".graph"));

        OutputBitStream graphOut = new OutputBitStream(new FileOutputStream(newBasename + ".graph"));
        OutputBitStream offsetsOut = new OutputBitStream(new FileOutputStream(newBasename + ".offsets"));

        Properties prop1 = new Properties(), prop2 = new Properties();
        prop1.load(new FileInputStream(g1 + ".properties"));
        prop2.load(new FileInputStream(g2 + ".properties"));
        long n1 = Long.parseLong(prop1.getProperty("nodes"));
        long n2 = Long.parseLong(prop2.getProperty("nodes"));
        int intervalLength1 = Integer.parseInt(prop1.getProperty("minintervallength"));
        int intervalLength2 = Integer.parseInt(prop2.getProperty("minintervallength"));
        nodes = Math.max(n1,n2);

        long prevWrittenBits = 0;

        long node = 0;
        try {
            for (node = 0; node < nodes; node++) {
                if(node == 2324)
                    System.out.println("h");
                int out1 = node < n1 ? graph1Stream.readGamma() : 0;
                int out2 = node < n2 ? graph2Stream.readGamma() : 0;
                offsetsOut.writeLongGamma(graphOut.writtenBits() - prevWrittenBits);
                prevWrittenBits = graphOut.writtenBits();
                if (out1 == 0 && out2 == 0) {
                    graphOut.writeGamma(0);
                    continue;
                }
                int nodesLeft1 = out1, nodesLeft2 = out2;

                int intervalCount1 = nodesLeft1 > 0 ? graph1Stream.readGamma() : 0;
                int intervalCount2 = nodesLeft2 > 0 ? graph2Stream.readGamma() : 0;
                long[][] intervals1 = new long[intervalCount1][2], intervals2 = new long[intervalCount2][2];
                nodesLeft1 -= readIntervals(intervalCount1, intervals1, node, graph1Stream, intervalLength1);
                nodesLeft2 -= readIntervals(intervalCount2, intervals2, node, graph2Stream, intervalLength2);
                long[][] resultingIntervals = new long[intervalCount1 + intervalCount2][2];
                Pair<Integer, Integer> result = mergeIntervals(intervals1, intervals2, resultingIntervals);
                int numIntervals = result.fst, numNodesInIntervals = result.snd;
                intervalisedarcs += numNodesInIntervals;

                long[] residuals = new long[nodesLeft1 + nodesLeft2];
                int numResiduals = readResiduals(graph1Stream, nodesLeft1, graph2Stream, nodesLeft2, node, residuals, resultingIntervals);
                residualarcs += numResiduals;

                arcs += numResiduals+numNodesInIntervals;
                graphOut.writeGamma(numResiduals + numNodesInIntervals); //outdegree

                graphOut.writeGamma(numIntervals);
                for (int i = 0; i < numIntervals; i++) {
                    if (i == 0)
                        graphOut.writeLongGamma(Fast.int2nat(resultingIntervals[i][0] - node));
                    else
                        graphOut.writeLongGamma(resultingIntervals[i][0] - resultingIntervals[i - 1][1] - 1);
                    graphOut.writeLongGamma(resultingIntervals[i][1] - resultingIntervals[i][0] - minintervallength);
                }

                for (int i = 0; i < numResiduals; i++) {
                    if (i == 0)
                        graphOut.writeLongZeta(Fast.int2nat(residuals[i] - node), 3);
                    else
                        graphOut.writeLongZeta(residuals[i] - residuals[i - 1] - 1, 3);
                }
            }
        }catch (IllegalArgumentException e){
            System.out.println("Crashed on node " + node);
            e.printStackTrace();
        }

        offsetsOut.writeLongGamma(graphOut.writtenBits()- prevWrittenBits);
        graphOut.close();
        offsetsOut.close();
        graph1Stream.close();
        graph2Stream.close();

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
        FileOutputStream propOut = new FileOutputStream(new File(newBasename + ".properties"));
        prop.store(propOut,"#BVGraph properties");
        propOut.close();

    }

    // The fst contains the next node, the second contains the new nodesRead
    private static Pair<Long,Integer> readNextUnused(InputBitStream g, int nodesLeft, int nodesRead, long node,
                                                     long[][] intervalizedNodes, long prevNode) throws IOException {
        long nextUnused = prevNode;
        if(nodesRead < nodesLeft) {
            nextUnused = nodesRead == 0 ? Fast.nat2int(g.readLongZeta(3)) + node : g.readLongZeta(3) + nextUnused + 1;
            while (isInIntervals(nextUnused, intervalizedNodes) && ++nodesRead < nodesLeft) {
                nextUnused = g.readLongZeta(3) + nextUnused + 1;
            }
        }
        if(nodesRead >= nodesLeft) //If there are no more nodes from graph one we should always take from the other graph
            nextUnused = Long.MAX_VALUE;
        return new Pair<>(nextUnused,nodesRead);
    }

    private static int readResiduals(InputBitStream g1, int nodesLeft1, InputBitStream g2, int nodesLeft2, long node,
                                     long[] residuals, long[][] intervalizedNodes) throws IOException {
        long n1 = Long.MAX_VALUE, n2 = Long.MAX_VALUE;
        int nodesRead1 = 0, nodesRead2 = 0;
        int i = 0;
        boolean used1 = true, used2 = true;
        while(nodesRead1 < nodesLeft1 || nodesRead2 < nodesLeft2){
            if(used1) { //the current node was used, read new node
                used1 = false;
                Pair<Long,Integer> res = readNextUnused(g1,nodesLeft1,nodesRead1,node,intervalizedNodes,n1);
                n1 = res.fst; nodesRead1 = res.snd;
            }
            if(used2) {
                used2 = false;
                Pair<Long,Integer> res = readNextUnused(g2,nodesLeft2,nodesRead2,node,intervalizedNodes,n2);
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

    protected static boolean isInIntervals(long value, long[][] intervals){
        int i = 0;
        while(i < intervals.length && intervals[i][1] < value) i++;
        return i < intervals.length && intervals[i][0] <= value && intervals[i][1] > value;
    }

    protected static boolean intervalsCanBeJoint(long[] interval1, long[] interval2){
        return interval1[0] <= interval2[1] && interval1[1] >= interval2[0];
    }

    protected static boolean tryToJoinIntervals(long[] intervalToJoinTo, long[] intervalToJoinFrom){
        if(intervalsCanBeJoint(intervalToJoinTo,intervalToJoinFrom)){
            intervalToJoinTo[0] = Math.min(intervalToJoinTo[0],intervalToJoinFrom[0]);
            intervalToJoinTo[1] = Math.max(intervalToJoinTo[1],intervalToJoinFrom[1]);
            return true;
        }
        return false;
    }

    // First is the number of intervals and second is the number of nodes included in the intervals
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

}
