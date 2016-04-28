package se.meltwater.graph;

import com.javamex.classmexer.MemoryUtil;
import it.unimi.dsi.big.webgraph.*;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.logging.ProgressLogger;
import se.meltwater.utils.Utils;

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

    private int unionVsGraphMemoryRatioThreashold = 2;
    private final int dataStructureOverheadFactor = 10;
    private long graphHeapUsageBytes;
    private long additionalGraphHeapUsageBytes;


    public ImmutableGraphWrapper(ImmutableGraph graph) {
        thisID = graphID++;
        additionalEdges = new SimulatedGraph();
        originalGraph = graph;
        this.graph = graph;

        graphHeapUsageBytes = Utils.getMemoryUsage(graph);
        additionalGraphHeapUsageBytes = Utils.getMemoryUsage(additionalEdges);
    }

    public void close(){
        if(thisPath != null){
            new File(thisPath + ".graph").delete();
            new File(thisPath + ".properties").delete();
            new File(thisPath + ".offsets").delete();
        }
    }

    /**
     * Inserts {@code edges} in to {@code currentAdditions} and
     * creates a graph of their union.
     * @param currentAdditions A graph to union the edges with
     * @param edges The edges to be unioned
     * @return A graph containing the union
     */
    private ImmutableGraph unionEdges(SimulatedGraph currentAdditions, Edge ... edges) {
        try {
            currentAdditions.addEdges(edges);
            return new UnionImmutableGraph(originalGraph, new SimulatedGraphWrapper(currentAdditions));
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Explicitly says that the graph should be stored
     * instead of letting the class decide when storage
     * should occur. Mainly used for testing and benchmarking.
     * @param edges The edges to add
     */
    public void addEdgesStored(Edge ... edges) {
        try {
            graph = unionEdges(additionalEdges, edges);
            storeGraphs(graph);

        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Stores the graph on disk and reloads it.
     * This removes possible overheads from the graph,
     * such as unions. Updates any graph references.
     * @param graph The graph to store and reload
     */
    private void storeGraphs(ImmutableGraph graph) {
        System.out.println("storing");
        try {
            checkFile();
            BVGraph.store(graph, thisPath, 0, 0, -1, -1, 0);
            this.graph = BVGraph.load(thisPath);
            originalGraph = this.graph;
            cleanOldFile(oldPath);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void store(String outputFile) {
        try {
            BVGraph.store(graph, outputFile, 0, 0, -1, -1, 0, new ProgressLogger());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    /**
     * Add {@code edges} to the graph. The additional edges will be saved in an
     * external data structure which is eventually saved to disk.
     */
    public boolean addEdges(Edge ... edges) {
        long addedEdgesHeapUsageBytes = Utils.getMemoryUsage(edges) * dataStructureOverheadFactor;
        additionalGraphHeapUsageBytes += addedEdgesHeapUsageBytes;
        float unionVsGraphMemoryRatio = (additionalGraphHeapUsageBytes) / (float)graphHeapUsageBytes;

        graph = unionEdges(additionalEdges, edges);

        if(unionVsGraphMemoryRatio > unionVsGraphMemoryRatioThreashold) {
            storeGraphs(graph);

            additionalEdges = new SimulatedGraph();
            additionalGraphHeapUsageBytes = Utils.getMemoryUsage(additionalEdges);
            graphHeapUsageBytes = Utils.getMemoryUsage(graph);
        }

        return true;
    }



    private void checkFile() throws IOException {
        if(tempDir == null)
            tempDir = new File(Files.createTempDirectory("tmpGraphs").toAbsolutePath().toUri());
        oldPath = thisPath;
        thisPath = tempDir.getAbsolutePath() + "/graph_" + thisID + "_" + fileVersion++;
    }

    private void cleanOldFile(String oldPath){
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

    @Override
    /**
     * Transposes the graph and stores it to a memory mapped file.
     * @return The transposed graph
     */
    public IGraph transpose(){
        try {
            ImmutableGraph transpose = Transform.transposeOffline(graph, (int) graph.numNodes(), null, new ProgressLogger());

            ImmutableGraphWrapper transposeWrapper = new ImmutableGraphWrapper(transpose);
            transposeWrapper.storeGraphs(transpose);

            return transposeWrapper;
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
}
