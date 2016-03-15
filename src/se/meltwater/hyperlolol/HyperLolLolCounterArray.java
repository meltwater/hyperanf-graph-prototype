package se.meltwater.hyperlolol;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.util.HyperLogLogCounterArray;

import java.util.Arrays;

/**
 * An increment-only dynamic version of HyperLogLogCounterArray.
 * Its purpose is to be able to use a HyperLogLogCounterArray when
 * the number of nodes is unknown.
 * It overrides the registers of HyperLogLogCounterArray.
 * The extended functionality is not thread-safe.
 *
 */
public class HyperLolLolCounterArray extends HyperLogLogCounterArray {

    protected long size;
    protected long limit;

    /**
     * {@code bits} and {@code registers} share the same memory
     */
    protected long[][] bits;
    protected LongBigList[] registers;

    protected long sentinelMask;

    private final float resizeFactor = 1.1f;

    private String exceptionString = "Exception in " + HyperLolLolCounterArray.class + ". ";

    public HyperLolLolCounterArray(long arraySize, long n, int log2m) {
        super(0, n, log2m);
        size = arraySize;
        limit = arraySize == 0 ? 1 : arraySize;
        sentinelMask = 1L << ( 1 << registerSize ) - 2;

        final long sizeInRegisters = limit * m;
        final int numVectors = getNumVectors(sizeInRegisters);

        initBitArrays(numVectors, sizeInRegisters);
    }

    /**
     * Allocates memory for the bit arrays {@code bits} {@code registers}
     * and initiates them with shared memory.
     *
     * @param numVectors The number of vectors required
     * @param sizeInRegisters The total size required (#counters * #registers)
     */
    private void initBitArrays(int numVectors, long sizeInRegisters) {
        bits = new long[ numVectors ][];
        registers = new LongBigList[ numVectors ];

        for( int i = 0; i < numVectors; i++ ) {
            final LongArrayBitVector bitVector = LongArrayBitVector.ofLength( registerSize * Math.min( CHUNK_SIZE, sizeInRegisters - ( (long)i << CHUNK_SHIFT ) ) );
            bits[ i ] = bitVector.bits();
            registers[ i ] = bitVector.asLongBigList( registerSize );
        }
    }

    /**
     * Requests that the HyperLolLol counter needs
     * {@code numberOfNewCounters} more counters.
     * Will increase the counter array if necessary.
     * @param numberOfNewCounters Number of new counters needed
     */
    public void addCounters(long numberOfNewCounters ) {
        if(numberOfNewCounters < 0) {
            throw new IllegalArgumentException(exceptionString + "Requested a negative number of new counters.");
        }

        if(size + numberOfNewCounters > limit) {
            increaseNumberOfCounters(numberOfNewCounters);
        }

        size += numberOfNewCounters;
    }

    /**
     * Calculates the new array size and calls for a resize.
     * Guarantees that at least the number of new counters
     * requested will fit into the array.
     * @param numberOfNewCounters Number of new counters needed
     */
    private void increaseNumberOfCounters(long numberOfNewCounters) {
        double resizePow = Math.ceil(Math.log((limit + numberOfNewCounters) / (float)limit ) * (1/Math.log(resizeFactor)));

        long newLimit = (long)(limit * Math.pow(resizeFactor, resizePow));
        resizeCounterArray(newLimit);
        limit = newLimit;
    }

    /**
     *
     * @param newArraySize Must be strictly larger than previous size
     * @throws IllegalArgumentException If size is smaller then previous
     */
    private void resizeCounterArray(long newArraySize) throws IllegalArgumentException {
        if(limit > newArraySize) {
            throw new IllegalArgumentException(exceptionString + "A smaller array size: " + newArraySize + " than previous " + limit + " was requested.");
        }

        final long sizeInRegisters = newArraySize * m;
        final int numVectors = getNumVectors(sizeInRegisters);

        long oldBits[][] = bits;

        initBitArrays(numVectors, sizeInRegisters);
        copyOldArraysIntoNew(oldBits);
    }

    /**
     * Calculates the number of vectors required to encode {@code sizeInRegisters}
     * @param sizeInRegisters The total size required (#counters * #registers)
     * @return the number of vectors required
     */
    private int getNumVectors(long sizeInRegisters) {
        return  (int)( ( sizeInRegisters + CHUNK_MASK ) >>> CHUNK_SHIFT );
    }


    /**
     * Copies {@code oldBits} into class variable {@code bits}
     * @param oldBits Bits to be copied
     */
    private void copyOldArraysIntoNew(long[][] oldBits) {
        for(int i = 0; i < oldBits.length; i++) {
            for(int j = 0 ; j < oldBits[i].length; j++) {
                bits[i][j] = oldBits[i][j];
            }
        }
    }


    /** Clears all registers. */
    @Override
    public void clear() {
        for( long[] a: bits ) Arrays.fill( a, 0 );
    }


    /** Estimates the number of distinct elements that have been added to a given counter so far.
     *
     * @param k the index of the counter.
     * @return an approximation of the number of distinct elements that have been added to counter <code>k</code> so far.
     */
    @Override
    public double count( final long k ) {
        return count( bits[ chunk( k ) ], offset( k ) );
    }

    public long getJenkinsSeed(){
        return seed;
    }

    /** Adds an element to a counter.
     *
     * @param k the index of the counter.
     * @param v the element to be added.
     */
    @Override
    public void add( final long k, final long v ) {
        final long x = jenkins( v, seed );
        final int j = (int)( x & mMinus1 );
        final int r = Long.numberOfTrailingZeros( x >>> log2m | sentinelMask );
        final LongBigList l = registers[ chunk( k ) ];
        final long offset = ( ( k << log2m ) + j ) & CHUNK_MASK;
        l.set( offset, Math.max( r + 1, l.getLong( offset ) ) );
    }

    public void clearCounter(long index){
        long[] list = bits[chunk(index)];
        long offset = offset(index);
        long remaining = registerSize*m;      // The remaining number of bits to be cleared
        long fromRight = offset % Long.SIZE;  // The offset in the current long from {the least significant bit}
                                              // that should be cleared. We see the least signifcant bit to be the "rightmost" bit
        long mask = (1L << fromRight) - 1L;   // All zeroes from the most significant bit up to the bit at fromRight.
                                              // The rest is ones.
        int word = (int) (offset / Long.SIZE);// The long to be edited
        while(remaining > 0) { // Still have bits to clear
            // mask currently contains all bits to the left of fromRight. If those zeroes are more than the number of
            // remaining bits we have to set some bits to one.
            if (remaining < Long.SIZE - fromRight)
                // We want to set all bits from the most significant bit up to bit remaining+fromRight to one.
                // To do this we create a mask with zeroes up to bit remaining+fromRight and the rest ones.
                // We then take the bitwise compliment of this mask and /or/ it with the original mask.
                mask |= ~((1L << (fromRight + remaining)) - 1L);
            list[word++] &= mask; // clear the zeroed bits in the current long
            remaining -= Long.SIZE-fromRight;
            mask = 0; // Only the first iteration will have a fromRight offset
            fromRight = 0;
        }

    }

    public void union(long index, HyperLolLolCounterArray from){
        long[] chunk = bits[chunk(index)];
        long offset = offset(index);
        long remaining = registerSize*m;      // The remaining number of bits to be cleared
        int fromRight = (int)(offset % Long.SIZE);  // The offset in the current long from {the least significant bit}
        // that should be cleared. We see the least signifcant bit to be the "rightmost" bit
        long mask = (1L << fromRight) - 1L;   // All zeroes from the most significant bit up to the bit at fromRight.
        // The rest is ones.
        int word = (int) (offset / Long.SIZE);// The long to be edited
        long[] temp = new long[counterLongwords];
        this.getLolLolCounter(index, temp);
        long[] temp2 = new long[counterLongwords];
        from.getLolLolCounter(index, temp2);
        max(temp,temp2);
        temp[temp.length-1] = temp[temp.length-1];
        if(fromRight == 0) {
            long endMask = remaining % Long.SIZE == 0 ? ~0 : (1L << (remaining % Long.SIZE)) - 1L;
            if(remaining % Long.SIZE == 0)
                System.out.println("yo, yoyo");
            for(int i = 0; i <temp.length-1; i++) {
                chunk[word + i] = temp[i];
            }
            chunk[word + temp.length-1] = chunk[word + temp.length-1] & ~endMask | temp[temp.length-1] & endMask;
        }else{
            System.out.println("nemen");

            long startMask = ~((1L << fromRight) - 1L);
            chunk[word] = chunk[word] & ~startMask | temp[0] << fromRight;
            long carry = temp[0] >>> Long.SIZE - fromRight;
            int carryLength = fromRight;
            remaining -= Long.SIZE - fromRight;

            int i = 1;
            while(remaining >= 64){
                chunk[word+i] = temp[i] << carryLength | carry;
                carry = temp[i] >>> Long.SIZE - carryLength;
                remaining -= 64;
                i++;
            }
            if(remaining > 0)
                chunk[i] = chunk[i] & ~((1L << remaining) - 1L) | (temp[i] << carryLength | carry) & ((1L << remaining) - 1L);



        }


    }

    public static long jenkins( final long x, final long seed ) {
        long a, b, c;

		/* Set up the internal state */
        a = seed + x;
        b = seed;
        c = 0x9e3779b97f4a7c13L; /* the golden ratio; an arbitrary value */

        a -= b; a -= c; a ^= (c >>> 43);
        b -= c; b -= a; b ^= (a << 9);
        c -= a; c -= b; c ^= (b >>> 8);
        a -= b; a -= c; a ^= (c >>> 38);
        b -= c; b -= a; b ^= (a << 23);
        c -= a; c -= b; c ^= (b >>> 5);
        a -= b; a -= c; a ^= (c >>> 35);
        b -= c; b -= a; b ^= (a << 49);
        c -= a; c -= b; c ^= (b >>> 11);
        a -= b; a -= c; a ^= (c >>> 12);
        b -= c; b -= a; b ^= (a << 18);
        c -= a; c -= b; c ^= (b >>> 22);

        return c;
    }

    public final void getLolLolCounter(long index, long[] dest) {
        this.getCounter(this.bits[this.chunk(index)], index, dest);
    }

    public void setJenkinsSeed(long seed){
        this.seed = seed;
    }
}
