
import com.sun.org.apache.xpath.internal.SourceTree;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.LazyIntIterator;
import se.meltwater.Converter;
import se.meltwater.GraphChanger;
import se.meltwater.GraphReader;

import java.util.ArrayList;

/**
 * Created by simon on 2016-02-17.
 */
public class Main {

    private final static String[] DEFAULT_FLAGS = {"-u"};

    public static void main(String[] args) throws Exception {
        if(args.length == 0)
            args = DEFAULT_FLAGS;

        if(args[0].equals("-u") && args.length == 5 && args[1].equals("-w") )
            new Converter().union(args[2],args[3],args[4]);
        else if(args[0].equals("-u") && args.length == 4 && !args[1].equals("-w")) {
            System.out.println("Starting internal merge");
            long time = System.currentTimeMillis();
            GraphChanger.merge(args[1], args[2], args[3]);
            System.out.println("Finished internal merge in " + (System.currentTimeMillis() - time) + "ms.");
        }else if(args[0].equals("-g") && args.length == 2){
            String[] genArgs = {"-o","-O","-L", args[1]};
            BVGraph.main(genArgs);
        }else if(args[0].equals("-rb") && args.length == 3) {
            BVGraph graph = BVGraph.loadMapped(args[1]);
            BVGraph.store(graph, args[2], 0, 0, -1, -1, 0);
        }else if(args[0].startsWith("-r") && args.length == 3){
            boolean print = !args[0].equals("-rnp");
            GraphReader.readGraph(args[1],Integer.parseInt(args[2]),print);
        }else {
            printUsages();
        }
    }

    private static void printIntervals(long[][] intervals){
        for(long[] interval : intervals)
            System.out.print("(" + interval[0] + "," + interval[1] + ")");
        System.out.println();
    }

    private static void printUsages(){
        System.out.println("Join two graphs to one: -u [-w] <graph1basename> <graph2basename> <newBasename>");
        System.out.println("The flag -w says that webgraphs method will be used.");
        System.out.println("Generate BVGraph from .graph: -g <graphBasename>");
        System.out.println("Remove blocks from graph: -rb <graphBasename> <newBasename>");
        System.out.println("Read graph: -r <basename> <nodes>");
    }

}
