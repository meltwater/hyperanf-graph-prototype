package se.meltwater.examples;


import se.meltwater.algo.DANF;
import se.meltwater.algo.HyperBoll;
import se.meltwater.graph.Edge;
import se.meltwater.graph.SimulatedGraph;
import se.meltwater.vertexcover.DynamicVertexCover;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class Benchmarks {

    public static void benchmark() throws IOException, InterruptedException {
        int log2m = 7;
        int h = 3;

        SimulatedGraph graph = new SimulatedGraph();
        graph.addNode(0);

        DynamicVertexCover dvc = new DynamicVertexCover(graph);

        DANF danf = new DANF(dvc, h, graph);
        HyperBoll hyperBoll = new HyperBoll(graph, log2m);

        hyperBoll.init();
        for (int i = 1; i <= h; i++) {
            hyperBoll.iterate();
            danf.addHistory(hyperBoll.getCounter(), i);
        }

        hyperBoll.close();

        PrintWriter writer = new PrintWriter("benchmarksprev.data");

        Random rand = new Random();
        int added = 0;
        long lastTime = System.currentTimeMillis();
        long startTime = lastTime;
        while(System.in.available() == 0 && added < 1000000) {
            int bulkSize = 10000;
            Edge[] edges = new Edge[bulkSize];
            for (int i = 0; i < bulkSize; i++) {
                long from = rand.nextInt((int)graph.getNumberOfNodes() + 100);
                long to = rand.nextInt((int)graph.getNumberOfNodes() + 100);
                edges[i] = new Edge(from, to);
            }
            danf.addEdges(edges);
            added += bulkSize;

            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - lastTime;
            float timePerBulkSeconds = elapsedTime / 1000.0f;
            float dps = bulkSize / timePerBulkSeconds;

            long heapSize = Runtime.getRuntime().totalMemory();

            lastTime = currentTime;

            float elapsedTimeSinceStart = (currentTime - startTime) / 1000.0f;

            System.out.print("Total nr edges: " + added + ". ");
            System.out.print("Added " + bulkSize + " edges. ");
            System.out.print("Time per bulk: " + timePerBulkSeconds + "s. ");
            System.out.print("DPS: " + dps + ". ");
            System.out.print("Heap size: " + heapSize + " bytes. ");
            System.out.println("Total time: " + elapsedTimeSinceStart + "s.");


            writer.println(added + " " + dps + " " + heapSize);
        }

        writer.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Benchmarks.benchmark();
    }

}
