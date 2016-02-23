package se.meltwater;

import com.martiansoftware.jsap.JSAPException;
import it.unimi.dsi.webgraph.BVGraph;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by johan on 2016-02-23.
 */
public class BVConverter {




    public static void main(String[] args) throws Exception {
        String graphFileName = "/home/johan/programming/master/it/unimi/dsi/webgraph/graphs/twitter-2010";
        String[] argz = {"-o", "-O", "-L", graphFileName};
        BVGraph.main(argz);
    }
}
