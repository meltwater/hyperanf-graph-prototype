package se.meltwater.test.hllresize;

import org.junit.Test;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Created by johan on 2016-03-01.
 */
public class TestHyperLolol {

    final int maxCounters = 100;
    final int log2mMinSize = 4;

    final int nrTestIterations = 100;

    private int arraySize;
    private int n;
    private int log2m;
    private Random rand;
    private int increaseSize;
    private HyperLolLolCounterArray counter;



    @Test
    public void testNewPartsCanBeUsedOfResize() {
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            setupParameters();

            counter.addCounters(increaseSize);
            randomlyAddHashesToCounters(arraySize + increaseSize);

            assertAllCountersLargerThanZero(arraySize + increaseSize);
        }
    }

    @Test
    public void testResizeKeepsPreviousValues() throws IOException {
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            setupParameters();

            randomlyAddHashesToCounters(arraySize);

            double[] prevCounts = getCurrentCountersAsList(arraySize);

            counter.addCounters(increaseSize);

            assertPreviousValuesAreIntact(arraySize, prevCounts);
            assertNewValuesAreZero(arraySize + increaseSize, increaseSize);
        }
    }

    @Test
    public void testManyIncreases() {
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            setupParameters();

            randomlyAddHashesToCounters(arraySize);

            double[] prevCounts = getCurrentCountersAsList(arraySize);

            for(int i = 0; i < increaseSize - 1; ) {
                int currentIncreaseSize = rand.nextInt(increaseSize - i) + 1;
                counter.addCounters(currentIncreaseSize);

                assertPreviousValuesAreIntact(arraySize, prevCounts);

                i += currentIncreaseSize;
            }
        }
    }

    @Test
    public void testClearCounter(){
        int iteration = 0;
        while(iteration++ < nrTestIterations){
            setupParameters();
            counter.addCounters(1);
            long index = arraySize == 0 ? 0 : rand.nextInt(arraySize);
            counter.add(index, 5);
            assertTrue(counter.count(index) > 0);
            counter.clearCounter(index);
            assertEquals("Iteration " + iteration + " failed.",0, counter.count(index), 0.01);
        }
    }

    @Test
    public void testUnion(){
        int iteration = 0;
        while(iteration++ < nrTestIterations){
            setupParameters();
            HyperLolLolCounterArray counter2 = new HyperLolLolCounterArray(arraySize,n,log2m);
            counter2.setJenkinsSeed(counter.getJenkinsSeed());
            counter.addCounters(5);
            counter2.addCounters(5);
            long[][] stuff = randomlyAddHashesToCounters(arraySize+5);
            long[][] moreStuff = randomlyAddHashesToCounters(arraySize+5,counter2);
            HyperLolLolCounterArray counterOriginal = new HyperLolLolCounterArray(arraySize,n,log2m);
            counterOriginal.addCounters(5);
            counterOriginal.setJenkinsSeed(counter.getJenkinsSeed());
            for(int i=0; i<stuff.length; i++){
                for(int j=0; j<stuff[i].length; j++)
                    counterOriginal.add(i,stuff[i][j]);
            }
            /*long[][] maxHashes = new long[1 << log2m][stuff.length];
            long[][] maxHashes2 = new long[1 << log2m][moreStuff.length];
            for(int i = 0; i < stuff.length; i++){
                maxHashes[i] = 0;
                for(int j=0; j < stuff[i].length; j++){
                    long thisHash = HyperLolLolCounterArray.jenkins(stuff[i][j],counter.getJenkinsSeed());
                    thisHash = thisHash >>> log2m | (1L << ( 1 << HyperLolLolCounterArray.registerSize(n) ) - 2);
                    maxHashes[i] = Math.max(Long.numberOfTrailingZeros(thisHash),maxHashes[i]);
                }
            }
            for(int i = 0; i < moreStuff.length; i++){
                maxHashes2[i] = 0;
                for(int j=0; j < moreStuff[i].length; j++){
                    long thisHash = HyperLolLolCounterArray.jenkins(moreStuff[i][j],counter2.getJenkinsSeed());
                    thisHash = thisHash >>> log2m | (1L << ( 1 << HyperLolLolCounterArray.registerSize(n) ) - 2);
                    maxHashes2[i] = Math.max(Long.numberOfTrailingZeros(thisHash),maxHashes2[i]);
                }
            }*/
            long unionedNode = (long)rand.nextInt(arraySize+5);
            counter.union(unionedNode, counter2);
            long[] bits1 = new long[counter.counterLongwords];
            long[] bits2 = new long[counter.counterLongwords];
            long[] bitsOriginal1 = new long[counter.counterLongwords];
            for(int i = 0; i< stuff.length; i++){
                Arrays.fill(bits1,0);
                Arrays.fill(bits2,0);
                Arrays.fill(bitsOriginal1,0);
                counter.getLolLolCounter(i,bits1);
                counter2.getLolLolCounter(i,bits2);
                counterOriginal.getLolLolCounter(i,bitsOriginal1);
                if(i != unionedNode){
                    assertArrayEquals(bits1,bitsOriginal1);
                }else{
                    long valMask = (1 << counter.registerSize) - 1;
                    long carry1 = 0, carry2 = 0, carryOriginal1 = 0;
                    int carryLength = 0;
                    for(int j=0; j<bitsOriginal1.length; j++){
                        long valOriginal1 = bitsOriginal1[j];
                        long val2 = bits2[j];
                        long val1 = bits1[j];
                        int remaining = Long.SIZE;
                        while(remaining >= counter.registerSize) {
                            int bitsToConsume =  counter.registerSize - carryLength;
                            assertEquals(((val1 << carryLength) | carry1) & valMask,
                                Math.max(((val2 << carryLength) | carry2) & valMask,
                                         ((valOriginal1 << carryLength) | carryOriginal1) & valMask));
                            valOriginal1 >>>= bitsToConsume;
                            val1 >>>= bitsToConsume;
                            val2 >>>= bitsToConsume;
                            remaining -= bitsToConsume;
                            carry1 = carry2 = carryOriginal1 = 0;
                            carryLength = 0;
                        }
                        carryLength = remaining;
                        carry1 = val1;
                        carry2 = val2;
                        carryOriginal1 = valOriginal1;
                    }

                }
            }
        }
    }

    @Test
    public void testUnionNotSmaller(){
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            setupParameters();
            HyperLolLolCounterArray counter2 = new HyperLolLolCounterArray(arraySize, n, log2m);
            counter2.setJenkinsSeed(counter.getJenkinsSeed());
            counter.addCounters(5);
            counter2.addCounters(5);
            randomlyAddHashesToCounters(arraySize + 5);
            randomlyAddHashesToCounters(arraySize + 5, counter2);
            long unionedNode = (long) rand.nextInt(arraySize + 3) + 1;
            double countNodeBefore = counter.count(unionedNode-1);
            double countBefore = counter.count(unionedNode);
            double countNodeAfter = counter.count(unionedNode+1);
            counter.union(unionedNode, counter2);
            double countAfter = counter.count(unionedNode);
            assertTrue("countBefore: " + countBefore + ", countAfter " + countAfter, countBefore <= countAfter);
            assertEquals(counter.count(unionedNode-1), countNodeBefore, 0.01);
            assertEquals(counter.count(unionedNode+1), countNodeAfter, 0.01);
        }
    }

    private void setupParameters() {
        rand = new Random();
        arraySize = rand.nextInt(maxCounters);
        n = maxCounters;
        log2m = rand.nextInt(10) + log2mMinSize;
        increaseSize = rand.nextInt(maxCounters - arraySize);
        counter = new HyperLolLolCounterArray(arraySize, n, log2m);
    }

    private void assertAllCountersLargerThanZero(int arraySize) {
        for(int i = 0; i < arraySize ; i++) {
            assertTrue(counter.count(i) > 0);
        }
    }

    private long[][] randomlyAddHashesToCounters(int numberOfCounters) {
        return randomlyAddHashesToCounters(numberOfCounters,counter);
    }

    private long[][] randomlyAddHashesToCounters(int numberOfCounters, HyperLolLolCounterArray counter) {
        int maxAddedValues = 100;
        long[][] ret = new long[numberOfCounters][maxAddedValues];
        for (int i = 0; i < numberOfCounters; i++) {
            for(int j = 0; j < maxAddedValues; j++) {
                int addedValue = rand.nextInt();
                counter.add(i, addedValue);
                ret[i][j] = addedValue;
            }
        }
        return ret;
    }

    private double[] getCurrentCountersAsList(int numberOfCounters) {
        double prevCounts[] = new double[numberOfCounters];
        for (int i = 0; i < numberOfCounters; i++) {
            prevCounts[i] = counter.count(i);
        }

        return prevCounts;
    }

    private void assertPreviousValuesAreIntact(int numberOfCounters, double[] prevCounts) {
        for (int i = 0; i < numberOfCounters; i++) {
            assertTrue(prevCounts[i] == counter.count(i));
        }
    }

    private void assertNewValuesAreZero(int numberOfCounters, int increaseSize) {
        for(int i = numberOfCounters - increaseSize; i < numberOfCounters; i++) {
            assertTrue(counter.count(i) == 0);
        }
    }
}

