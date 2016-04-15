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

    @Override
    public boolean equals(Object other){
        if(other == null)
            return false;

        if(!(other instanceof Edge))
            return false;

        Edge edge = (Edge)other;
        return edge.from == from && edge.to == to;
    }

    public Edge flip() {
        return new Edge(to, from);
    }

    @Override
    public String toString(){
        return "(" + from + ", " + to + ")";
    }

    public long from;
    public long to;
}
