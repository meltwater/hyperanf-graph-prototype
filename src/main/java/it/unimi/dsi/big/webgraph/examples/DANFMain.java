package it.unimi.dsi.big.webgraph.examples;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraphWrapper;
import it.unimi.dsi.big.webgraph.UnionImmutableGraph;
import it.unimi.dsi.big.webgraph.algo.MSBreadthFirst;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class DANFMain {

    private static String jarName = "jarname";
    private static String jarDescription  = "Provides a cli for running and testing individual parts of our dynamic-anf implementation.";

    private static String pathDescription = "The path to the basename file";

    private final static String[] DEFAULT_FLAGS = {"-u"};

    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            printUsages();
        }

        String[] argsWithoutFirstFlag = Arrays.copyOfRange(args, 1, args.length);

        switch (args[0]) {
            case "-vc":
                doVertexCover(argsWithoutFirstFlag);
                break;
            case "-g":
                doGenerateBvGraph(argsWithoutFirstFlag);
                break;
            case "-rb":
                doRemoveBlocks(argsWithoutFirstFlag);
                break;
            case "-bfs":
                doBFS(argsWithoutFirstFlag);
                break;
            case "-a":
                doBVGraphFromAscii(argsWithoutFirstFlag);
                break;
            default:
                printUsages();
                System.exit(0);
        }
    }

    private static void doBVGraphFromAscii(String[] args) throws JSAPException, IllegalAccessException, IOException, InstantiationException, NoSuchMethodException, InvocationTargetException, ClassNotFoundException {

        SimpleJSAP jsap = new SimpleJSAP(jarName, jarDescription,
                new Parameter[] {
                        new FlaggedOption("arc-list path",JSAP.STRING_PARSER,null,JSAP.REQUIRED,'a',"arc-list path", pathDescription),
                        new FlaggedOption("output bvgraph path",JSAP.STRING_PARSER,null,JSAP.REQUIRED,'b',"output bvgraph path", pathDescription),

                }
        );

        JSAPResult result = jsap.parse(args);
        checkErrorFlags(jsap,result);

        String[] bvArgs = new String[]{"-g", "ArcListASCIIGraph", result.getString("arc-list path"), result.getString("output bvgraph path")};
        BVGraph.main(bvArgs);

    }

    private static void doBFS(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP(jarName, jarDescription,
                new Parameter[] {
                        new UnflaggedOption("filename", JSAP.STRING_PARSER, true,
                                "The graph to perform the Breadth First Search on")
                }
        );

        JSAPResult result = jsap.parse(args);
        checkErrorFlags(jsap, result);

        String graphName = result.getString("filename");

        BVGraph graph = BVGraph.loadMapped(graphName);
        long[] bfsSources = new long[1000];
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        for(int i=0; i<1000 ; i++)
            bfsSources[i] = rand.nextLong(graph.numNodes());

        MSBreadthFirst MSBFS = new MSBreadthFirst(new ImmutableGraphWrapper(graph));

        MSBFS.breadthFirstSearch(bfsSources);
        MSBFS.close();

    }

    private static void doVertexCover(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP(jarName, jarDescription,
                new Parameter[] {
                        new FlaggedOption( "path", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'p', "path", pathDescription)
                }
        );

        JSAPResult result = jsap.parse(args);
        checkErrorFlags(jsap, result);

        String path = result.getString("path");

        VertexCoverExample vertexCoverExample = new VertexCoverExample(path);
        vertexCoverExample.run();
    }

    private static  void doGenerateBvGraph(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP(jarName, jarDescription,
                new Parameter[] {
                        new FlaggedOption( "path", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'p', "path", pathDescription)
                }
        );

        JSAPResult result = jsap.parse(args);
        checkErrorFlags(jsap, result);

        String path = result.getString("path");

        String[] genArgs = {"-o","-O","-L", path};
        BVGraph.main(genArgs);
    }

    private static void doRemoveBlocks(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP(jarName, jarDescription,
                new Parameter[] {
                        new FlaggedOption( "inpath",  JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'i', "inpath",  pathDescription),
                        new FlaggedOption( "outpath", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'o', "outpath", pathDescription)
                }
        );

        JSAPResult result = jsap.parse(args);
        checkErrorFlags(jsap, result);

        String inpath  = result.getString("inpath");
        String outpath = result.getString("outpath");

        BVGraph graph = BVGraph.loadMapped(inpath);
        BVGraph.store(graph, outpath, 0, 0, -1, -1, 0);
    }


    private static void checkErrorFlags(JSAP jsap, JSAPResult result) {
        if(!result.success()) {
            System.err.println("Usage: java " + jarName);
            System.err.println("                " + jsap.getUsage());
            System.exit(1);
        }
    }


    private static void printIntervals(long[][] intervals){
        for(long[] interval : intervals)
            System.out.print("(" + interval[0] + "," + interval[1] + ")");
        System.out.println();
    }

    private static void printUsages(){
        System.out.println("-g   : Generate BVGraph from .graph");
        System.out.println("-rb  : Remove blocks from graph");
        System.out.println("-vc  : Calculate a 2-approximate vertex cover in graph");
        System.out.println("-bfs : Do a random Multi-Source Breadth-First search");
        System.out.println("-a   : Generate BVGraph from Ascii graph");
    }

    public static void union(String graph1, String graph2, String newName) throws IOException {
        BVGraph g1 = BVGraph.loadMapped(graph1), g2 = BVGraph.loadMapped(graph2);
        System.out.println("Started joining graphs.");
        long time = System.currentTimeMillis();
        BVGraph.store(new UnionImmutableGraph(g1,g2),newName);
        System.out.println("Joined graphs in: " + (System.currentTimeMillis() - time) + "ms.");
    }
}
