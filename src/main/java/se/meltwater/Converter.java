package se.meltwater;

import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.UnionImmutableGraph;

import java.io.IOException;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * // TODO class description
 */
public class Converter {


    static public void main(String[] args) throws IOException {
    }

    public static void convert(String arcList,String bvgraphName) {
        String[] argz = {"-w","0","-g", "ArcListASCIIGraph", arcList, bvgraphName};

        try {
            BVGraph.main(argz);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void union(String graph1, String graph2, String newName) throws IOException {
        BVGraph g1 = BVGraph.loadMapped(graph1), g2 = BVGraph.loadMapped(graph2);
        System.out.println("Started joining graphs.");
        long time = System.currentTimeMillis();
        BVGraph.store(new UnionImmutableGraph(g1,g2),newName);
        System.out.println("Joined graphs in: " + (System.currentTimeMillis() - time) + "ms.");
    }
}
