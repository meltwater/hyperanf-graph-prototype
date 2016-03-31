package se.meltwater.graph;

import it.unimi.dsi.big.webgraph.*;
import it.unimi.dsi.big.webgraph.ArcListASCIIGraph;
import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.big.webgraph.Transform;
import it.unimi.dsi.big.webgraph.UnionImmutableGraph;
import it.unimi.dsi.io.ByteBufferInputStream;
import it.unimi.dsi.webgraph.*;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;

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
    private long modifications = 0;
    private String oldPath = null;
    private static File tempDir = null;
    private String thisPath = null;
    private static int graphID = 0;
    private int thisID;
    private int fileVersion = 0;

    public ImmutableGraphWrapper(ImmutableGraph graph) {
        this.graph = graph;
        thisID = graphID++;
    }

    public void close(){
        if(thisPath != null){
            new File(thisPath + ".graph").delete();
            new File(thisPath + ".properties").delete();
            new File(thisPath + ".offsets").delete();
        }
    }

    @Override
    public boolean addEdges(Edge ... edges){

        try {
            SimulatedGraph extraEdge = new SimulatedGraph();
            extraEdge.addEdges(edges);
            ImmutableGraph store = new UnionImmutableGraph(new SimulatedGraphWrapper(extraEdge), graph);
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
            return new ImmutableGraphWrapper(Transform.transposeOffline(graph, (int) graph.numNodes()));
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
