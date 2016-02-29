package se.meltwater;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.*;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by simon on 2016-02-08.
 */
public class GraphReader {

    private final int NUM_NODES = 4;

    public static void main(String[] args) throws Exception{


    }

    public static void readOffsets(){

        try {

            int[] expectedOutDegrees = {8,3,1,1,1,0,0,0,0,0};
            InputBitStream ibs = new InputBitStream(new File("graphs/union.offsets"));
            InputBitStream graphIbs = new InputBitStream(new File("graphs/union.graph"));
            int offset = 0;
            for (int node = 0; node < expectedOutDegrees.length; node++) {
                offset = ibs.readGamma() + offset;
                graphIbs.position(offset);
                int out = graphIbs.readGamma();
                if(out != expectedOutDegrees[node])
                    System.out.println("Something wrong for node : " + node + ". Degree was "
                                       + out + " but should be " + expectedOutDegrees[node]);
                else
                    System.out.println("Correct for node " + node);
            }
            System.out.println("Total length: " + (ibs.readGamma() + offset));

        }catch (FileNotFoundException e){
            System.err.println("Couldn't find file: " + e.getMessage());
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    public static void readGraph(String graph, int num_nodes, boolean print) {

        /*
        offset: gamma (default)
        outdegree: gamma (default)
        reference: unary (default)
        block: gamma (default)
        block count: gamma (default)
        residual: zeta (default)
        intervalCount: gamma (always)
        intervalNumbers: gamma (always)
         */
        int node = 0;
        try {
            InputBitStream ibs = new InputBitStream(new File(graph + ".graph"));
            //Map<Integer,ArrayList<Integer>> arcs = new HashMap<>();
            for(node = 0; node < num_nodes; node++) {
                int out = ibs.readGamma();
                ArrayList<Integer> neighbors = new ArrayList<>(out);
                if(out == 0){
                    if(print)
                        System.out.println("node: " + node + " has no neighbours.");
                    //arcs.put(node,neighbors);
                    continue;
                }
                int nodesLeft = out;

                int blockCount = -1, reference = -1;
                /*if(false) {
                    reference = ibs.readUnary();
                    if (reference != 0) {
                        blockCount = ibs.readGamma();
                        int refIndex = node - reference; //The index to the referenced node
                        nodesLeft -= readBlock(neighbors, arcs.get(refIndex), ibs, blockCount);
                    }
                }*/

                int intervalCount = -1;

                if(nodesLeft > 0) {
                    intervalCount = ibs.readGamma();
                    if (intervalCount > 0) {
                        nodesLeft -= readIntervals(ibs,intervalCount,node,neighbors);
                    }
                }

                if(nodesLeft > 0) {

                    readResiduals(ibs,neighbors,node,nodesLeft);

                }

                //arcs.put(node,neighbors);
                if(print)
                    System.out.println("node: " + node + ", out: " + out + ", ref: " + reference + ", blockCount: " + blockCount + ", intervalCount: " + intervalCount + ", neighbors: " + neighbors);
            }

        }catch (FileNotFoundException e){
            System.err.println("Couldn't find file: " + e.getMessage());
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    private static int readResiduals(InputBitStream ibs, ArrayList<Integer> neighbors, int node, int nodesLeft) throws IOException {

        int suc1 = Fast.nat2int(ibs.readZeta(3)) + node;
        neighbors.add(suc1);
        nodesLeft--;
        int prev = suc1;
        while (nodesLeft-- > 0) {
            int thisNode = ibs.readZeta(3) + prev + 1;
            neighbors.add(thisNode);
            prev = thisNode;
        }
        return 0;

    }

    private static int readIntervals(InputBitStream ibs, int intervalCount, int node, ArrayList<Integer> neighbors) throws IOException {
        int nodesRead = 0,from = 0,length = 0;
        boolean first = true;
        while(intervalCount-- > 0){
            if(first){
                from = Fast.nat2int(ibs.readGamma()) + node;
                first = false;
            }else
                from = ibs.readGamma() + from + length + 1;

            length = ibs.readGamma()+4;
            for (int i = 0; i < length; i++) {
                neighbors.add(i + from);
                nodesRead++;
            }
        }
        return nodesRead;
    }

    /**
     * Reads the block of referenced neighbors. Assumes there is one and that the /ibs/ is at the correct position.
     * @param neighbors
     * @param copyFrom
     * @param ibs
     * @param blockCount
     * @return Number of nodes read
     * @throws IOException
     */
    private static int readBlock(ArrayList<Integer> neighbors, ArrayList<Integer> copyFrom, InputBitStream ibs,
                          int blockCount) throws IOException {
        int nodesRead = 0;
        int refNeighPos = 0;
        for(int i = 0; i < blockCount ; i += 2){
            int copyLength = ibs.readGamma(); // length of block
            neighbors.addAll(copyFrom.subList(refNeighPos,refNeighPos+copyLength));
            refNeighPos += copyLength;
            nodesRead += copyLength;
            if(i+1 < blockCount) //The next int says how many we should ignore so we just move the pointer.
                refNeighPos += ibs.readGamma();
        }
        if(blockCount % 2 == 0){ //If there was an even number of blocks, then the rest of the neighbors
                                 //should be copied as well.
            neighbors.addAll(refNeighPos,copyFrom);
            nodesRead += copyFrom.size()-refNeighPos;
        }
        return nodesRead;
    }

}
