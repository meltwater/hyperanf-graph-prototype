package se.meltwater.vertexcover;

import se.meltwater.graph.Edge;

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
    Map<Long, AffectedState> deleteEdge(Edge edge);

    boolean isInVertexCover(long node);

    int getVertexCoverSize();

}
