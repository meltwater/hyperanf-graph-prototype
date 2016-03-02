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
 *
 */
public class HyperLolLolCounterArray extends HyperLogLogCounterArray {

    protected long size;
    protected long limit;

    protected long[][] bits;
    protected LongBigList[] registers;

    protected long sentinelMask;

    private final float resizeSize = 1 + (1 / 10);

    private String exceptionString = "Exception in " + HyperLolLolCounterArray.class + ". ";

    public HyperLolLolCounterArray(long arraySize, long n, int log2m) {
        super(0, n, log2m);
        size = arraySize;
        limit = arraySize;
        sentinelMask = 1L << ( 1 << registerSize ) - 2;

        final long sizeInRegisters = arraySize * m;
        final int numVectors = getNumVectors(sizeInRegisters);

        bits = new long[ numVectors ][];
        registers = new LongBigList[ numVectors ];

        initBitArrays(bits, registers, numVectors, sizeInRegisters);
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

        if(size + numberOfNewCounters >= limit) {
            increaseNumberOfCounters(numberOfNewCounters);
        }

        size += numberOfNewCounters;
    }

    private void increaseNumberOfCounters(long increaseSize) {
        long newLimit = (long)(limit * resizeSize) + increaseSize;
        resizeCounterArray(newLimit);
        limit = newLimit;
    }

    private void resizeCounterArray(long newArraySize) throws IllegalArgumentException {
        if(limit > newArraySize) {
            throw new IllegalArgumentException(exceptionString + "Requested a smaller array size than previous.");
        }

        final long sizeInRegisters = newArraySize * m;
        final int numVectors = getNumVectors(sizeInRegisters);

        long newBits[][] = new long[ numVectors ][];
        LongBigList newRegisters[] = new LongBigList[ numVectors ];

        initBitArrays(newBits, newRegisters, numVectors, sizeInRegisters);
        copyOldArraysIntoNew(newBits);

        bits = newBits;
        registers = newRegisters;
    }

    private int getNumVectors(long sizeInRegisters) {
        return  (int)( ( sizeInRegisters + CHUNK_MASK ) >>> CHUNK_SHIFT );
    }

    private void initBitArrays(long[][] bits, LongBigList[] registers, int numVectors, long sizeInRegisters) {
        for( int i = 0; i < numVectors; i++ ) {
            final LongArrayBitVector bitVector = LongArrayBitVector.ofLength( registerSize * Math.min( CHUNK_SIZE, sizeInRegisters - ( (long)i << CHUNK_SHIFT ) ) );
            bits[ i ] = bitVector.bits();
            registers[ i ] = bitVector.asLongBigList( registerSize );
        }
    }

    private void copyOldArraysIntoNew(long[][] newBits) {
        for(int i = 0; i < bits.length; i++) {
            for(int j = 0 ; j < bits[i].length; j++) {
                newBits[i][j] = bits[i][j];
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


    private final static long jenkins( final long x, final long seed ) {
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
}
