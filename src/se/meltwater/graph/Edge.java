package se.meltwater.graph;

/**
 * Created by johan on 2016-02-26.
 */
public class Edge {

    public Edge(Node from, Node to) {
        this.from = from;
        this.to = to;
    }

    public Node from;
    public Node to;
}
