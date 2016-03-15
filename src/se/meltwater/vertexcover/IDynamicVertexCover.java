package se.meltwater.vertexcover;

import se.meltwater.graph.Edge;

/**
 * Created by johan on 2016-02-26.
 */
public interface IDynamicVertexCover {

    void insertEdge(Edge edge);
    void deleteEdge(Edge edge);

    boolean isInVertexCover(long node);

    long[] getNodesInVertexCover();

    int getVertexCoverSize();

}
