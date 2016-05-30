package it.unimi.dsi.big.webgraph.algo;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.big.webgraph.Edge;
import it.unimi.dsi.big.webgraph.MutableGraph;

import java.util.Map;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * // TODO class description
 */
public interface IDynamicVertexCover {

    enum AffectedState {Added, Removed};

    Map<Long, AffectedState> insertEdge(Edge edge);
    Map<Long, AffectedState> deleteEdge(Edge edge, MutableGraph graphTranspose);

    boolean isInVertexCover(long node);

    LongArrayBitVector getNodesInVertexCover();
    LazyLongIterator getNodesInVertexCoverIterator();

    long getVertexCoverSize();

    long getMemoryUsageBytes();

}
