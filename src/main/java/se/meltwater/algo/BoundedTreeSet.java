package se.meltwater.algo;

import java.util.Comparator;
import java.util.TreeSet;

/**
 * // TODO Class description
 *
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 */
public class BoundedTreeSet<E> extends TreeSet<E>{

    private int maxCapacity;

    public BoundedTreeSet(int maxCapacity) {
        super();
        this.maxCapacity = maxCapacity;
    }

    public BoundedTreeSet(int maxCapacity, Comparator<? super E> comparator) {
        super(comparator);
        this.maxCapacity = maxCapacity;
    }

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
}
