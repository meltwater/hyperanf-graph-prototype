package se.meltwater.hyperlolol;

import com.sun.xml.internal.bind.annotation.OverrideAnnotationOf;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.util.HyperLogLogCounterArray;

import java.util.Arrays;

/**
 * Created by johan on 2016-03-01.
 */
public class HyperLolLolCounterArray extends HyperLogLogCounterArray {


    protected long size;
    protected long limit;

    protected long[][] bits;
    protected LongBigList[] registers;

    private long sentinelMask;

    public HyperLolLolCounterArray(long arraySize, long n, int log2m) {
        super(0, 0, log2m);
        size = arraySize;
        limit = arraySize;
        sentinelMask = 1L << ( 1 << registerSize ) - 2;

        final long sizeInRegisters = arraySize * m;
        final int numVectors = (int)( ( sizeInRegisters + CHUNK_MASK ) >>> CHUNK_SHIFT );

        bits = new long[ numVectors ][];
        registers = new LongBigList[ numVectors ];

        for( int i = 0; i < numVectors; i++ ) {
            final LongArrayBitVector bitVector = LongArrayBitVector.ofLength( registerSize * Math.min( CHUNK_SIZE, sizeInRegisters - ( (long)i << CHUNK_SHIFT ) ) );
            bits[ i ] = bitVector.bits();
            registers[ i ] = bitVector.asLongBigList( registerSize );
        }
    }

    public void addCounters(long increaseSize) {
        if(size + increaseSize >= limit) {
            resizeCounterArray(size + increaseSize);
        }
    }

    private void resizeCounterArray(long arraySize) throws IllegalArgumentException {
        if(size > arraySize) {
            throw new IllegalArgumentException("Exception in HyperLololCounter. Cant downsize array size");
        }

        final long sizeInRegisters = arraySize * m;
        final int numVectors = (int)( ( sizeInRegisters + CHUNK_MASK ) >>> CHUNK_SHIFT );

        long newbits[][] = new long[ numVectors ][];
        LongBigList newregisters[] = new LongBigList[ numVectors ];

        for( int i = 0 ; i < numVectors; i++ ) {
            final LongArrayBitVector bitVector = LongArrayBitVector.ofLength( registerSize * Math.min( CHUNK_SIZE, sizeInRegisters - ( (long)i << CHUNK_SHIFT ) ) );
            newbits[ i ] = bitVector.bits();
            newregisters[ i ] = bitVector.asLongBigList( registerSize );
        }

        copyOldArraysIntoNew(newbits);

        bits = newbits;
        registers = newregisters;
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
