
import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.BVGraph;
import se.meltwater.Converter;
import se.meltwater.GraphChanger;
import se.meltwater.GraphReader;
import se.meltwater.examples.VertexCover;

import java.util.Arrays;

/**
 * Created by simon on 2016-02-17.
 */
public class Main {

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
            case "-u":
                doUnion(argsWithoutFirstFlag);
                break;
            case "-vc":
                doVertexCover(argsWithoutFirstFlag);
                break;
            case "-g":
                doGenerateBvGraph(argsWithoutFirstFlag);
                break;
            case "-rb":
                doRemoveBlocks(argsWithoutFirstFlag);
                break;
            case "-r":
                doReadGraph(argsWithoutFirstFlag);
                break;
            default:
                printUsages();
                System.exit(0);
        }
    }

    private static void doUnion(String[] args) throws Exception {
        SimpleJSAP jsap = new SimpleJSAP(jarName, jarDescription,
                new Parameter[] {
                        new Switch("webgraph", 'w', "webgraph", "Specify to use webgraphs original union"),
                        new UnflaggedOption("filenames", JSAP.STRING_PARSER, true,
                                        "The filepaths to the graphs to be unioned. " +
                                        "Format ingraph1 ingraph2 outgraph").setGreedy(true)
                }
        );

        JSAPResult result = jsap.parse(args);
        checkErrorFlags(jsap, result);

        boolean useWebgraph = result.getBoolean("webgraph");
        String[] filepaths = result.getStringArray("filenames");

        if(useWebgraph){
            new Converter().union(filepaths[0], filepaths[1], filepaths[2]);
        } else {
            long time = System.currentTimeMillis();
            GraphChanger.merge(filepaths[0], filepaths[1], filepaths[2]);
            System.out.println("Finished internal merge in " + (System.currentTimeMillis() - time) + "ms.");
        }
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

        VertexCover vertexCover = new VertexCover(path);
        vertexCover.run();
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

    private static void doReadGraph(String[] args) throws  Exception {
        SimpleJSAP jsap = new SimpleJSAP(jarName, jarDescription,
                new Parameter[] {
                        new Switch("print", 'e', "print", "Prints graph if set"),
                        new FlaggedOption("path",  JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'p', "path",  pathDescription),
                        new FlaggedOption("numnodes", JSAP.INTEGER_PARSER, null, JSAP.REQUIRED, 'n', "numnodes", "The number of nodes to read"),
                }
        );

        JSAPResult result = jsap.parse(args);
        checkErrorFlags(jsap, result);

        boolean print = result.getBoolean("print");
        String path = result.getString("path");
        int numNodes = result.getInt("numnodes");

        GraphReader.readGraph(path, numNodes ,print);

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
        System.out.println("-g  : Generate BVGraph from .graph");
        System.out.println("-r  : Read graph");
        System.out.println("-rb : Remove blocks from graph");
        System.out.println("-u  : Join two graphs to one");
        System.out.println("-vc : Calculate a 2-approximate vertex cover in graph");
    }

}
