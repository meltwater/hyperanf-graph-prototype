package it.unimi.dsi.big.webgraph.algo;

import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.MutableGraph;
import it.unimi.dsi.bits.LongArrayBitVector;

import java.util.Map;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public interface IDynamicVertexCover {

    enum AffectedState {ADDED, REMOVED}

    Map<Long, AffectedState> insertEdge(Edge edge);
    Map<Long, AffectedState> deleteEdge(Edge edge, MutableGraph graphTranspose);

    boolean isInVertexCover(long node);

    LongArrayBitVector getNodesInVertexCover();
    LazyLongIterator getNodesInVertexCoverIterator();

    long getVertexCoverSize();

    long getMemoryUsageBytes();
}
