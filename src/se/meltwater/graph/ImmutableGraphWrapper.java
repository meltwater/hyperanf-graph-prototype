package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.*;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.function.BiFunction;

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
    private ImmutableGraph originalGraph;
    private SimulatedGraph additionalEdges;
    private long modifications = 0;
    private String oldPath = null;
    private static File tempDir = null;
    private String thisPath = null;
    private static int graphID = 0;
    private int thisID;
    private int fileVersion = 0;

    public ImmutableGraphWrapper(ImmutableGraph graph) {
        thisID = graphID++;
        additionalEdges = new SimulatedGraph();
        originalGraph = graph;
        this.graph = graph;
    }

    public void close(){
        if(thisPath != null){
            new File(thisPath + ".graph").delete();
            new File(thisPath + ".properties").delete();
            new File(thisPath + ".offsets").delete();
        }
    }

    /**
     * Method only used for comparing Unioned vs Stored graphs
     * @param edges
     * @return
     */
    public boolean addEdgesUnioned(Edge ... edges) {

        try {
            additionalEdges.addEdges(edges);
            graph = new UnionImmutableGraph(originalGraph,new SimulatedGraphWrapper(additionalEdges));

            return true;
        }catch (Exception e){
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean addEdges(Edge ... edges){

        try {
            SimulatedGraph g = new SimulatedGraph();
            g.addEdges(edges);
            ImmutableGraph store = new UnionImmutableGraph(new SimulatedGraphWrapper(g), graph);
            checkFile();
            BVGraph.store(store, thisPath, 0, 0, -1, -1, 0);
            graph = BVGraph.load(thisPath);
            cleanOldFile();
            return true;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private void checkFile() throws IOException {
        if(tempDir == null)
            tempDir = new File(Files.createTempDirectory("tmpGraphs").toAbsolutePath().toUri());
        oldPath = thisPath;
        thisPath = tempDir.getAbsolutePath() + "/graph_" + thisID + "_" + fileVersion++;
    }

    private void cleanOldFile(){
        if(oldPath != null) {
            new File(oldPath + ".graph").delete();
            new File(oldPath + ".properties").delete();
            new File(oldPath + ".offsets").delete();
        }
    }

    @Override
    public boolean addEdge(Edge edge){

        if(containsEdge(edge))
            return false;

        modifications++;

        SimulatedGraph extraEdge = new SimulatedGraph();
        extraEdge.addEdge(edge);
        graph = new UnionImmutableGraph(new SimulatedGraphWrapper(extraEdge),graph);
        return true;

    }

    private boolean containsEdge(Edge edge){
        if(containsNode(edge.from) && containsNode(edge.to)) {
            LazyLongIterator nodeIt = getSuccessors(edge.from);
            long degree = getOutdegree(edge.from);
            while (degree-- > 0)
                if (nodeIt.nextLong() == edge.to)
                    return true;
        }
        return false;
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
        return new ImmutableGraphIteratorWrapper(graph.nodeIterator(node));
    }

    @Override
    public void merge(IGraph graph){
        if(graph instanceof ImmutableGraphWrapper){
            ImmutableGraph copy = ((ImmutableGraphWrapper)graph).graph;
            this.graph = new UnionImmutableGraph(this.graph,copy);
        }else
            super.merge(graph);
    }

    public IGraph transpose(){
        try {
            ImmutableGraph transpose = Transform.transposeOffline(graph, (int) graph.numNodes());
            BVGraph.store(transpose, "tmp", 0, 0, -1, -1, 0);
            transpose = BVGraph.load("tmp"); // TODO Use javas tmp files


            return new ImmutableGraphWrapper(transpose);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public NodeIterator getNodeIterator(){
        return getNodeIterator(0);
    }

    private class ImmutableGraphIteratorWrapper extends NodeIterator {

        private final NodeIterator it;
        private final long expectedModifications;

        ImmutableGraphIteratorWrapper(NodeIterator iterator){
            it = iterator;
            expectedModifications = modifications;
        }

        @Override
        public long outdegree() {
            checkValidState();
            return it.outdegree();
        }

        @Override
        public long nextLong(){
            checkValidState();
            return it.nextLong();
        }

        @Override
        public boolean hasNext() {
            checkValidState();
            return it.hasNext();
        }

        @Override
        public LazyLongIterator successors(){
            checkValidState();
            return it.successors();
        }

        private void checkValidState(){
            if(modifications != expectedModifications)
                throw new ConcurrentModificationException("Immutable graph wrapper must never be modified during iteration.");
        }
    }



    private class SimulatedGraphWrapper extends ImmutableGraph{

        SimulatedGraph graph;

        public SimulatedGraphWrapper(SimulatedGraph graph){
            this.graph = graph;
        }

        @Override
        public long numArcs() {
            return graph.getNumberOfArcs();
        }

        @Override
        public LazyLongIterator successors(long x) {
            return graph.getSuccessors(x);
        }

        @Override
        public NodeIterator nodeIterator(long from) {
            return graph.getNodeIterator(from);
        }

        @Override
        public long numNodes() {
            return graph.getNumberOfNodes();
        }

        @Override
        public boolean randomAccess() {
            return true;
        }

        @Override
        public long outdegree(long l) {
            return graph.getOutdegree(l);
        }

        @Override
        public ImmutableGraph copy() {
            return new SimulatedGraphWrapper((SimulatedGraph) graph.copy());
        }
    }

}
