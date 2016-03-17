package se.meltwater.graph;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * Class for containing a directed edge
 */
public class Edge {

    public Edge(long from, long to) {
        this.from = from;
        this.to = to;
    }

    public long from;
    public long to;
}
