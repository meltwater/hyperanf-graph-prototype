package se.meltwater;

import com.martiansoftware.jsap.JSAPException;
import it.unimi.dsi.webgraph.BVGraph;

/**
 * Created by johan on 2016-01-28.
 */
public class Converter {


    static public void main(String[] args) {
        Converter converter = new Converter();
        converter.convert();
    }

    public void convert() {
        String[] argz = {"-g", "ArcListASCIIGraph", "files/graph.arcs", "bvgraph"};

        try {
            BVGraph.main(argz);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
