package se.meltwater.vertexcover;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import se.meltwater.graph.Edge;
import se.meltwater.graph.IGraph;

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
    Map<Long, AffectedState> deleteEdge(Edge edge, IGraph graphTranspose);

    boolean isInVertexCover(long node);

    LongArrayBitVector getNodesInVertexCover();
    LazyLongIterator getNodesInVertexCoverIterator();

    long getVertexCoverSize();

}
