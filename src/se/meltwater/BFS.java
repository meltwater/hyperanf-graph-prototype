package se.meltwater;

import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by johan on 2016-02-24.
 */
public class BFS {

    public static void main(String[] args) throws IOException {
        String graphFileName = "/home/johan/programming/master/it/unimi/dsi/webgraph/graphs/it-2004";
        final ProgressLogger pl = new ProgressLogger();
        final ImmutableGraph graph = ImmutableGraph.load( graphFileName, pl );

        int n = graph.numNodes();

        final int lo = 0;
        final long hi = 100;

        int curr = lo, succ, ecc = 0, reachable = 0;

        final int maxDist = 7;

        final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
        final int[] dist = new int[ n ];
        Arrays.fill( dist, Integer.MAX_VALUE );

        long start = System.currentTimeMillis();
        for( int i = lo; i < hi; i++ ) {
            if ( dist[ i ] == Integer.MAX_VALUE ) { // Not already visited
                queue.enqueue( i );

                dist[ i ] = 0;

                LazyIntIterator successors;

                while( ! queue.isEmpty() ) {
                    curr = queue.dequeueInt();
                    successors = graph.successors( curr );
                    int d = graph.outdegree( curr );
                    while( d-- != 0 ) {
                        succ = successors.nextInt();
                        if ( dist[ succ ] == Integer.MAX_VALUE && dist[ curr ] + 1 <= maxDist ) {
                            reachable++;
                            dist[ succ ] = dist[ curr ] + 1;
                            ecc = Math.max( ecc, dist[ succ ] );
                            queue.enqueue( succ );
                        }
                    }
                }
            }
            pl.update();
        }

        long end = System.currentTimeMillis();
        System.out.println("Running time: " + (float)(end - start)/1000 );

    }

}
