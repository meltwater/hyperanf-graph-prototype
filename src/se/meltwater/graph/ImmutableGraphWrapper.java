package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.*;
import it.unimi.dsi.io.ByteBufferInputStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Wraps an Immutable graph to be able to use it
 * with the IGraph interface. The IGraph interface
 * is useful in testing as the type of the graph is now
 * abstracted away from the test cases.
 */
public class ImmutableGraphWrapper extends AGraph{

    private ImmutableGraph graph;
    private NodeIterator nodeIterator;
    LazyLongIterator successors;


    public ImmutableGraphWrapper(ImmutableGraph graph) {
        this.graph = graph;
    }

    @Override
    public boolean addEdge(Edge edge){
        if(containsNode(edge.from) && containsNode(edge.to)) {
            LazyLongIterator nodeIt = getSuccessors(edge.from);
            long degree = getOutdegree(edge.from);
            while (degree-- > 0)
                if (nodeIt.nextLong() == edge.to)
                    return false;
        }

        String strGraph = edge.from + " " + edge.to;
        try {
            ImmutableGraph extraEdge = new StringArcListASCIIGraph(strGraph,0, Math.max(edge.from,edge.to)+1);
            graph = new UnionImmutableGraph(graph,extraEdge);
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Got an IOException from reading a string...", e);
        }
    }

    @Override
    public IGraph copy(){
        return new ImmutableGraphWrapper(graph.copy());
    }

    @Override
    public long getOutdegree(long node){
        return graph.outdegree(node);
    }

    @Override
    public LazyLongIterator getSuccessors(long node){
        return graph.successors(node);
    }

    @Override
    public long getNumberOfNodes() {
        return graph.numNodes();
    }

    @Override
    public long getNumberOfArcs() { return graph.numArcs(); }

    @Override
    public NodeIterator getNodeIterator(long node){
        return graph.nodeIterator(node);
    }

    @Override
    public NodeIterator getNodeIterator(){
        return getNodeIterator(0);
    }

    private class StringArcListASCIIGraph extends ArcListASCIIGraph{

        private long numNodes;

        StringArcListASCIIGraph(final String graph, final int shift, final long numNodes) throws IOException {
            super(new ByteArrayInputStream(graph.getBytes()),shift);
            this.numNodes = numNodes;
        }

        @Override
        public long numNodes(){
            return numNodes;
        }

    }

}
