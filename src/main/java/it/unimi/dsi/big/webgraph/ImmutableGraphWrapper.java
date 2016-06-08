package it.unimi.dsi.big.webgraph;

import it.unimi.dsi.logging.ProgressLogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;

/**
 *
 * Creates a mutable graph of an immutable graph. This is done by keeping track of two graphs:
 * the original immutable graph and a {@link SimulatedGraph}. When edges are added to this
 * graph they are inserted into the simulated graph. A union is created by
 * {@link UnionImmutableGraph}. As the simulated graph quickly takes up a lot of memory
 * the unioned graph are eventually stored as a temporary BVGraph. This is done when
 * {@code unionVsGraphMemoryRatioThreshold < memoryUsage(simulatedGraph)/memoryUsage(unionedGraph)}.
 * The stored graph is then loaded as the original graph and the simulated graph is cleared.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 */
public class ImmutableGraphWrapper extends MutableGraph{

    private ImmutableGraph graph;
    private ImmutableGraph originalGraph;
    private SimulatedGraph additionalEdges;
    private String oldPath = null;
    private static File tempDir = null;
    private String thisPath = null;
    private static int graphID = 0;
    private int thisID;
    private int fileVersion = 0;
    private LoadMethod loadMethod;

    private float unionVsGraphMemoryRatioThreshold;
    private final int dataStructureOverheadFactor = 11;
    private long graphHeapUsageBytes;
    private long additionalGraphHeapUsageBytes;

    public final static float DEFAULT_UNION_VS_GRAPH_MEMORY_RATIO_THRESHOLD = 8.0f;
    public static final LoadMethod DEFAULT_LOAD_METHOD = LoadMethod.MAPPED;

    /**
     * Creates a mutable graph of an immutable graph.
     *
     * @param graph The immutable graph to wrap.
     */
    public ImmutableGraphWrapper(ImmutableGraph graph) {
        this(graph, DEFAULT_UNION_VS_GRAPH_MEMORY_RATIO_THRESHOLD);
    }

    /**
     * Creates a mutable graph of an immutable graph with the default load method
     * {@link ImmutableGraphWrapper#DEFAULT_LOAD_METHOD}.
     *
     * @param graph The immutable graph to wrap
     * @param unionVsGraphMemoryRatioThreshold The ratio at which the graph should be stored.
     */
    public ImmutableGraphWrapper(ImmutableGraph graph, float unionVsGraphMemoryRatioThreshold) {
        this(graph,unionVsGraphMemoryRatioThreshold,DEFAULT_LOAD_METHOD);
    }

    /**
     * Creates a mutable graph of an immutable graph with the default storage threshold
     * {@link ImmutableGraphWrapper#DEFAULT_UNION_VS_GRAPH_MEMORY_RATIO_THRESHOLD}.
     *
     * @param graph The immutable graph to wrap
     * @param loadMethod The method to use when re-loading the graph
     */
    public ImmutableGraphWrapper(ImmutableGraph graph, LoadMethod loadMethod){
        this(graph,DEFAULT_UNION_VS_GRAPH_MEMORY_RATIO_THRESHOLD,loadMethod);
    }

    /**
     * Creates a mutable graph of an immutable graph.
     *
     * @param graph The immutable graph to wrap
     * @param unionVsGraphMemoryRatioThreshold The ratio at which the graph should be stored.
     * @param loadMethod The method to use when re-loading the graph
     */
    public ImmutableGraphWrapper(ImmutableGraph graph, float unionVsGraphMemoryRatioThreshold, LoadMethod loadMethod){
        thisID = graphID++;
        additionalEdges = new SimulatedGraph();
        originalGraph = graph;
        this.graph = graph;
        this.unionVsGraphMemoryRatioThreshold = unionVsGraphMemoryRatioThreshold;

        graphHeapUsageBytes = Utils.getMemoryUsage(graph);
        additionalGraphHeapUsageBytes = Utils.getMemoryUsage(additionalEdges);
        this.loadMethod = loadMethod;

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
    private ImmutableGraph unionEdges(SimulatedGraph currentAdditions, Edge... edges) {
        try {
            currentAdditions.addEdges(edges);
            return new UnionImmutableGraph(originalGraph, currentAdditions);
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
            this.graph = loadGraph();
            originalGraph = this.graph;
            cleanOldFile(oldPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ImmutableGraph loadGraph() throws IOException {
        switch (loadMethod){
            case STANDARD:   return BVGraph.load(thisPath);
            case OFFLINE:    return BVGraph.loadOffline(thisPath);
            case SEQUENTIAL: return BVGraph.loadSequential(thisPath);
            case MAPPED:     return BVGraph.loadMapped(thisPath);
            case ONCE:       return BVGraph.loadOnce(new FileInputStream(thisPath));
            default:         throw new RuntimeException("Case missing");
        }
    }

    public void store(String outputFile) throws IOException {
        BVGraph.store(graph, outputFile, 0, 0, -1, -1, 0, new ProgressLogger());
    }

    public void setGraphHeapUsageBytes(long graphHeapUsageBytes) {
        this.graphHeapUsageBytes = graphHeapUsageBytes;
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

        if(unionVsGraphMemoryRatio > unionVsGraphMemoryRatioThreshold) {
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

        SimulatedGraph extraEdge = new SimulatedGraph();
        extraEdge.addEdge(edge);
        graph = new UnionImmutableGraph(extraEdge,graph);
        return true;

    }

    private boolean containsEdge(Edge edge){
        if(containsNode(edge.from) && containsNode(edge.to)) {
            LazyLongIterator nodeIt = successors(edge.from);
            long degree = outdegree(edge.from);
            while (degree-- > 0)
                if (nodeIt.nextLong() == edge.to)
                    return true;
        }
        return false;
    }

    @Override
    public MutableGraph copy(){
        return new ImmutableGraphWrapper(graph.copy(), unionVsGraphMemoryRatioThreshold);
    }

    @Override
    public long outdegree(long node){
        return graph.outdegree(node);
    }

    @Override
    public LazyLongIterator successors(long node){
        return graph.successors(node);
    }

    @Override
    public long numNodes() {
        return graph.numNodes();
    }

    @Override
    public long numArcs() { return graph.numArcs(); }

    @Override
    public boolean randomAccess() {
        return graph.randomAccess();
    }

    @Override
    public NodeIterator nodeIterator(long node){
        return graph.nodeIterator(node);
    }

    @Override
    /**
     * Transposes the graph and stores it to a memory mapped file.
     * @return The transposed graph
     */
    public MutableGraph transpose(){
        try {
            ImmutableGraph transpose = Transform.transposeOffline(graph, (int) graph.numNodes(), null, new ProgressLogger());

            ImmutableGraphWrapper transposeWrapper = new ImmutableGraphWrapper(transpose, unionVsGraphMemoryRatioThreshold);
            transposeWrapper.setGraphHeapUsageBytes(this.graphHeapUsageBytes);
            transposeWrapper.storeGraphs(transpose);

            return transposeWrapper;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public long getMemoryUsageBytes() {
        return Utils.getMemoryUsage(originalGraph) + Utils.getMemoryUsage(additionalEdges);
    }

    @Override
    public NodeIterator nodeIterator(){
        return nodeIterator(0);
    }

}
