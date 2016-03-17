package se.meltwater.test.hllresize;

import javafx.util.Pair;
import org.junit.Test;
import se.meltwater.hyperlolol.HyperLolLolCounterArray;

import java.io.IOException;
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
    /**
     * Tests that union a node from one HLL to another doesnt affect any other node than
     * the unioned and that the unioned node gets the correct value.
     *
     * Randomized tests that performs {@code nrTestIterations} number of
     * tests with random HLLs and a random node to union.
     */
    public void testUnion(){
        int iteration = 0;
        while(iteration++ < nrTestIterations){
            setupParameters();

            final int extraElementsInArray = 5;
            final int newArraySize = arraySize + extraElementsInArray;

            counter  = new HyperLolLolCounterArray(newArraySize,n,log2m);
            HyperLolLolCounterArray counter2 = new HyperLolLolCounterArray(newArraySize,n,log2m);
            HyperLolLolCounterArray counterOriginal = (HyperLolLolCounterArray) counter.clone();

            randomlyAddHashesToCounters(newArraySize);
            randomlyAddHashesToCounters(newArraySize, counter2);

            Pair<Long,Long> unionedNodes = unionRandomNode(newArraySize, counter, counter2);

            checkValues(counter, counter2, counterOriginal, newArraySize, unionedNodes.getKey(),unionedNodes.getValue());
        }
    }

    /**
     * Inserts all values in {@code values} into {@code counter}
     * @param values
     * @param counter
     */
    public void addValuesToHLL(long[][] values, HyperLolLolCounterArray counter) {
        for(int i=0; i<values.length; i++){
            for(int j=0; j<values[i].length; j++)
                counter.add(i,values[i][j]);
        }
    }

    /**
     * Randomly retreives a node and performs a union on that node index.
     * Takes the node from {@code from} and performs the union into {@code to}
     * @param newArraySize
     * @param to The HLL counter to be unioned
     * @param from
     * @return
     */
    private Pair<Long,Long> unionRandomNode(int newArraySize, HyperLolLolCounterArray to, HyperLolLolCounterArray from) {
        long unionedNode = (long)rand.nextInt(newArraySize);
        long fromNode = (long)rand.nextInt(newArraySize);
        to.union(unionedNode, from, fromNode);
        return new Pair<>(unionedNode,fromNode);
    }

    /**
     * Checks that the values after a union are correct. Counters that wasnt affected by the union
     * should be the same and counters that was affected by the union should have maxed the values
     * in its registers.
     * @param counter1
     * @param counter2
     * @param counterOriginal
     * @param arraySize
     * @param unionedNode
     */
    private void checkValues(HyperLolLolCounterArray counter1, HyperLolLolCounterArray counter2, HyperLolLolCounterArray counterOriginal,
                             int arraySize, long unionedNode, long unionFromNode) {
        long[] counter1Bits = new long[counter1.counterLongwords];
        long[] counter2Bits = new long[counter1.counterLongwords];
        long[] counter1OriginalBits = new long[counter1.counterLongwords];

        /* Compares all nodes in the counters */
        for (int i = 0; i < arraySize; i++) {
            counter1.getCounter(i,counter1Bits);
            counterOriginal.getCounter(i,counter1OriginalBits);

            if (i != unionedNode) {
                assertArrayEquals(counter1Bits,counter1OriginalBits);
            } else {
                counter2.getCounter(unionFromNode,counter2Bits);
                checkUnionedValue(counter1Bits, counter2Bits, counter1OriginalBits);
            }
        }
    }


    /**
     * Checks that the registers in {@code counter1Bits} have the max value of either {@code counter2Bits} or
     * {@code counter1OriginalBits}. Its purpose is to check that a counter union have correctly chosen the
     * max value of the unioned counters.
     * @param counter1Bits
     * @param counter2Bits
     * @param counter1OriginalBits
     */
    private void checkUnionedValue(long[] counter1Bits, long[] counter2Bits, long[] counter1OriginalBits) {
        long LSBToKeep = (1 << counter.registerSize) - 1;
        long carryCounter1 = 0, carryCounter2 = 0, carryCounterOriginal = 0;
        int carryLength = 0;

        /* Iterate all words of the counters */
        for (int i = 0; i < counter1OriginalBits.length; i++) {
            long valOriginal1 = counter1OriginalBits[i];
            long val2 = counter2Bits[i];
            long val1 = counter1Bits[i];

            int remaining = Long.SIZE;

            /* Iterate the registers in the current word */
            while (remaining >= counter.registerSize) {
                int bitsToConsume =  counter.registerSize - carryLength;

                /* Shift in possible carries from previous iterations and get the current register value */
                long registerValueCounter1 = ((val1 << carryLength) | carryCounter1) & LSBToKeep;
                long registerValueCounter2 = ((val2 << carryLength) | carryCounter2) & LSBToKeep;
                long registerValueCounterOriginal = ((valOriginal1 << carryLength) | carryCounterOriginal) & LSBToKeep;

                /* The union should have updated the register to use the max value of counter1 or counter2 */
                assertEquals( registerValueCounter1, Math.max(registerValueCounter2, registerValueCounterOriginal));

                /* Shift away the checked bits */
                valOriginal1 >>>= bitsToConsume;
                val1 >>>= bitsToConsume;
                val2 >>>= bitsToConsume;
                remaining -= bitsToConsume;

                /* Reset carries as we dont have any carries in the middle of a word */
                carryCounter1 = carryCounter2 = carryCounterOriginal = 0;
                carryLength = 0;
            }

            /* Set possible carries from the old word */
            carryLength = remaining;
            carryCounter1 = val1;
            carryCounter2 = val2;
            carryCounterOriginal = valOriginal1;
        }
    }

    @Test
    /**
     * Tests that a union between two HLL cannot lower the unioned node value.
     *
     * Randomized tests that performs {@code nrTestIterations} number of
     * tests with random HLLs and a random node to union.
     */
    public void testUnionNotSmaller(){
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            setupParameters();

            final int extraElementsInArray = 5;
            final int newArraySize = arraySize + extraElementsInArray;

            /* Init counters */
            counter = new HyperLolLolCounterArray(newArraySize, n, log2m);
            HyperLolLolCounterArray counter2 = new HyperLolLolCounterArray(newArraySize, n, log2m);
            HyperLolLolCounterArray counterOriginal = (HyperLolLolCounterArray) counter.clone();

            randomlyAddHashesToCounters(newArraySize, counter);
            randomlyAddHashesToCounters(newArraySize, counter2);

            final long unionedNode = (long) rand.nextInt(newArraySize), unionFrom = (long) rand.nextInt(newArraySize);

            counter.union(unionedNode, counter2, unionFrom);

            /* Make sure all counters are at least as big as before */
            for(int i = 0; i < newArraySize; i++) {
                double countBefore = counterOriginal.count(i);
                double countAfter = counter.count(i);

                assertTrue(countBefore <= countAfter);
            }

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

