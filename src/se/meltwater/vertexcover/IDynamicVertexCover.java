package se.meltwater.vertexcover;

import javafx.util.Pair;
import se.meltwater.graph.Edge;

import java.util.List;
import java.util.Map;

/**
 * Created by johan on 2016-02-26.
 */
public interface IDynamicVertexCover {

    enum AffectedState {Added, Removed};

    Map<Long, AffectedState> insertEdge(Edge edge);
    Map<Long, AffectedState> deleteEdge(Edge edge);

    boolean isInVertexCover(long node);

    int getVertexCoverSize();

}
