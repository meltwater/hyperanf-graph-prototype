package se.meltwater.vertexcover;

import se.meltwater.graph.Edge;
import se.meltwater.graph.Node;

/**
 * Created by johan on 2016-02-26.
 */
public interface IDynamicVertexCover {

    void insertEdge(Edge edge);
    void deleteEdge(Edge edge);

    boolean isInVertexCover(Node node);

}
