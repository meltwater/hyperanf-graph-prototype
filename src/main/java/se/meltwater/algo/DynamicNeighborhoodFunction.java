package se.meltwater.algo;

import se.meltwater.graph.Edge;

/**
 * TODO Class description
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public interface DynamicNeighborhoodFunction {

    void addEdges(Edge... edges);
    void close();

    long getMemoryUsageBytes();

}
