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

    /**
     * Take the union of the elements of {@code index} of this counter and {@code from}
     * and update this counter with the result. <b>WARNING: It is vital that both counters
     * have the same number of registers and the same register size. Make sure that the
     * counters are created with the same parameters to their constructors
     * @param index
     * @param from
     * @throws IllegalArgumentException If the counters didn't have the same parameters
     */
    public void union(long index, HyperLolLolCounterArray from) throws IllegalArgumentException{
        if(registerSize != from.registerSize || m != from.m ||
                offset(index) != from.offset(index) || chunk(index) != from.chunk(index)){
            throw new IllegalArgumentException("The counters to union between had different parameters " +
                                               "which this function can't handle.");
        }
        long[] chunk = bits[chunk(index)];
        long offset = offset(index);
        long remaining = registerSize*m;      // The remaining number of bits to be cleared
        int fromRight = (int)(offset % Long.SIZE);  // The offset in the current long from {the least significant bit}
        // that should be cleared. We see the least signifcant bit to be the "rightmost" bit
        int word = (int) (offset / Long.SIZE);// The long to be edited
        long[] temp = new long[counterLongwords];
        this.getLolLolCounter(index, temp);
        long[] temp2 = new long[counterLongwords];
        from.getLolLolCounter(index, temp2);
        max(temp,temp2); // union the counters

        if(fromRight == 0) { //No shift, we can just copy the data
            //endMask: The bits that are 1 are the bits we want to keep from the last element in temp
            long endMask = remaining % Long.SIZE == 0 ? ~0 : (1L << (remaining % Long.SIZE)) - 1L;
            System.arraycopy(temp,0,chunk,word,temp.length-1);
            chunk[word + temp.length-1] = chunk[word + temp.length-1] & ~endMask | temp[temp.length-1] & endMask;
        }else{
            copyShiftedArray(temp,0,chunk,word,remaining,fromRight);
        }
    }

    /**
     * Copies an array {@code src} where the values start at bit 0 (no shift) into an array
     * {@code dest} with all values shifted {@code shift} number of bits. The 0-bit in the
     * first word to be copied from {@code src} will be the {@code shift}-bit in the first
     * element to copy to in {@code dest}. All bits not copied to in {@code dest} will
     * remain unchanged.
     * @param dest    The destination array
     * @param destPos The index of the first word in {@code dest} to copy to
     * @param src     The array to copy from
     * @param srcPos  The index of the first word in {@code src} to copy from
     * @param numBits The total number of bits to be copied
     * @param shift   The bit position in the first word in {@code dest} that the bits in {@code src} should be copied to.
     */
    private static void copyShiftedArray(long[] src, int srcPos, long[] dest, int destPos, long numBits, int shift){
        // We want to keep the bits to the left of shift in the destination so we place them in the carry
        long startMask = (1L << shift) - 1L;
        long carry = dest[destPos] & startMask;

        int carryLength = shift;  // The carry length is the number of bits that have been shifted away
        long remaining = numBits;

        int i = srcPos;
        while(remaining >= Long.SIZE){ // The last word needs to be treated specially so we don't overwrite any data.
            dest[destPos+i] = src[i] << carryLength | carry; // add the bits in src along with the carry

            carry = src[i] >>> Long.SIZE - carryLength; // Set the new carry to the bits that weren't included

            // In the first iteration we only consume the bits that are to the left of shift
            remaining -= i == 0 ? Long.SIZE - shift : Long.SIZE;
            i++;
        }

        if(remaining > 0) {
            long bitsToKeep = ((1L << remaining) - 1L);
            // We keep the rest in the last element of the src array along with the carry.
            // As we know that the remaining bits are less than 64 we know that we won't shift away
            // relevant data. We also mask away any eventual scrap data from src (we only keep the last remaining bits).
            long dataToAdd = (src[i] << carryLength | carry) & bitsToKeep;
            // We don't want to delete the bits which are not in our range.
            long dataToKeep = dest[destPos + i] & ~bitsToKeep;
            dest[destPos + i] = dataToKeep | dataToAdd;
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
