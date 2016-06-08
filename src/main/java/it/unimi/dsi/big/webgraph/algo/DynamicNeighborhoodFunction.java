package it.unimi.dsi.big.webgraph.algo;

import it.unimi.dsi.big.webgraph.Edge;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public interface DynamicNeighborhoodFunction {

    void addEdges(Edge... edges);
    void close();

    long getMemoryUsageBytes();

}
