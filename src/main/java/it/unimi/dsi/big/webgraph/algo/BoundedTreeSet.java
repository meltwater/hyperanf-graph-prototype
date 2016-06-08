package it.unimi.dsi.big.webgraph.algo;

import java.util.Comparator;
import java.util.TreeSet;

/**
 * A BoundedTreeSet is a generic TreeSet with a
 * maxCapacity c. After c additions the last element
 * (according to the comparator) will be thrown away
 * at every addition.
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class BoundedTreeSet<E> extends TreeSet<E>{

    private int maxCapacity;

    /**
     * Creates a BoundedTreeSet that sorts elements
     * by the default comparator.
     * @param maxCapacity
     */
    public BoundedTreeSet(int maxCapacity) {
        super();
        this.maxCapacity = maxCapacity;
    }

    public BoundedTreeSet(int maxCapacity, Comparator<? super E> comparator) {
        super(comparator);
        this.maxCapacity = maxCapacity;
    }

    /**
     * Adds an element to the TreeSet. If the maximum capacity
     * is reached, the last element (by the comparator) will be thrown
     * away.
     * @param elem The element to insert
     * @return True if inserted.
     */
    @Override
    public boolean add(E elem) {
        boolean added = super.add(elem);

        if(super.size() > maxCapacity) {
            E lastElem = super.last();
            super.remove(lastElem);
            if(lastElem == elem) {
                return false;
            }
        }

        return added;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && obj instanceof BoundedTreeSet;
    }
}
