package se.meltwater;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.bits.Fast;
import it.unimi.dsi.io.InputBitStream;

import java.io.EOFException;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by simon on 2016-02-08.
 */
public class GraphReader {

    public static void main(String[] args) throws Exception{



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

        try {
            InputBitStream ibs = new InputBitStream(new File("graphs/bvgraph.graph"));
            Map<Integer,ArrayList<Integer>> arcs = new HashMap<>();
            for(int node = 0; node < 11; node++) {
                int out = ibs.readGamma();
                ArrayList<Integer> neighbors = new ArrayList<>(out);
                if(out == 0){
                    System.out.println("node: " + node + " has no neighbours.");
                    arcs.put(node,neighbors);
                    continue;
                }
                int nodesLeft = out;

                int reference = ibs.readUnary();
                int blockCount = -1;
                if(reference != 0) {
                    blockCount = ibs.readGamma();
                    int refIndex = node - reference; //The index to the referenced node
                    ArrayList<Integer> copyFrom = arcs.get(refIndex); //neighbors of referenced node
                    int refNeighPos = 0;
                    for(int i = 0; i < blockCount ; i += 2){
                        int copyLength = ibs.readGamma(); // length of block
                        neighbors.addAll(copyFrom.subList(refNeighPos,refNeighPos+copyLength));
                        refNeighPos += copyLength;
                        nodesLeft -= copyLength;
                        if(i+1 < blockCount) //The next int says how many we should ignore so we just move the pointer.
                            refNeighPos += ibs.readGamma();
                    }
                    if(blockCount % 2 == 0){ //If there was an even number of blocks, then the rest of the neighbors
                                             //should be copied as well.
                        neighbors.addAll(refNeighPos,copyFrom);
                        nodesLeft -= copyFrom.size()-refNeighPos;
                    }
                }

                int intervalCount = -1;

                if(nodesLeft > 0) {
                    intervalCount = ibs.readGamma();
                    if (intervalCount > 0) {
                        int from = Fast.nat2int(ibs.readGamma()) + node;
                        int length = ibs.readGamma() + 4;
                        for (int i = 0; i < length; i++) {
                            neighbors.add(i + from);
                            nodesLeft--;
                        }
                        while(--intervalCount > 0){
                            from = ibs.readGamma() + from + length + 1;
                            length = ibs.readGamma()+4;
                            for (int i = 0; i < length; i++) {
                                neighbors.add(i + from);
                                nodesLeft--;
                            }
                        }
                    }
                }

                if(nodesLeft > 0) {

                    int suc1 = Fast.nat2int(ibs.readZeta(3)) + node;
                    neighbors.add(suc1);
                    nodesLeft--;
                    int prev = suc1;
                    while (nodesLeft-- > 0) {
                        int thisNode = ibs.readZeta(3) + prev + 1;
                        neighbors.add(thisNode);
                        prev = thisNode;
                    }

                }

                arcs.put(node,neighbors);
                System.out.println("node: " + node + ", out: " + out + ", ref: " + reference + ", blockCount: " + blockCount + ", intervalCount: " + intervalCount + ", neighbors: " + neighbors);
            }

        }catch (EOFException e){}
    }

}
