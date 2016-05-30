package it.unimi.dsi.big.webgraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Function;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Abstract class that implements functions that
 * are common among all types of graphs.
 *
 * IGraphs purpose is to abstract what type of
 * graph the test suite is testing. In some cases
 * it is appropriate to use a physical file and in
 * other cases its not.
 */
public abstract class MutableGraph extends ImmutableGraph{


    public abstract boolean addEdge(Edge edge);
    public abstract boolean addEdges(Edge ... edges);

    public abstract MutableGraph transpose();

    public void store(String outputFile) throws IOException {
        BVGraph.store(this,outputFile);
    }

    static MutableGraph load(String inputFile, boolean memoryMapped) throws IOException {
        if(memoryMapped) {
            return new ImmutableGraphWrapper(ImmutableGraph.loadMapped(inputFile));
        } else {
            return new ImmutableGraphWrapper(ImmutableGraph.load(inputFile));
        }
    }

    public abstract long getMemoryUsageBytes();

    public boolean containsNode(long node) {
        return node < numNodes();
    }

    @Override
    /**
     * Two graphs are equal if they contain the same nodes and the same edges,
     * regardless their dynamic type class
     */
    public boolean equals(Object obj) {
        if(!(obj instanceof MutableGraph)) {
            return false;
        }

        MutableGraph otherGraph = (MutableGraph) obj;

        if(otherGraph.numNodes() != this.numNodes() ||
                otherGraph.numArcs()  != this.numArcs()) {
            return false;
        }

        ArrayList<Edge> edges = new ArrayList<>();
        iterateAllEdges(edge -> {
            edges.add(edge);
            return null;
        });

        otherGraph.iterateAllEdges(edge -> {
            edges.remove(edge);
            return null;
        });

        return edges.size() == 0;
    }
    /**
     * Iterates all edges in the graph and calls the lambda function for each edge.
     * The lambda function should return null if it should continue iterating
     * and a Object to indicate error. The Object will be returned.
     * @param function
     * @param <T> Return type
     * @return null if no error occurred, the error Object otherwise.
     */
    public <T> T iterateAllEdges(Function<Edge, T> function) {
        NodeIterator nodeIt = nodeIterator();
        long numNodes = numNodes();
        for(long node = 0; node < numNodes; node++) {
            nodeIt.nextLong();
            long outdegree = nodeIt.outdegree();
            LazyLongIterator neighbors = nodeIt.successors();

            while(outdegree != 0) {
                long neighbor = neighbors.nextLong();
                T returnedValue = function.apply(new Edge(node, neighbor));
                if(returnedValue != null) {
                    return returnedValue;
                }

                outdegree--;
            }
        }
        return null;
    }

}

