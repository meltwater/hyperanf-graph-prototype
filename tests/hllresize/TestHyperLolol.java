package hllresize;

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

            counter.increaseCounterSize(increaseSize);
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

            counter.increaseCounterSize(increaseSize);

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
                counter.increaseCounterSize(currentIncreaseSize);

                assertPreviousValuesAreIntact(arraySize, prevCounts);

                i += currentIncreaseSize;
            }
        }
    }

    private void setupParameters() {
        rand = new Random();
        arraySize = rand.nextInt(maxCounters);
        n = maxCounters * 2;
        log2m = rand.nextInt(10) + log2mMinSize;
        increaseSize = rand.nextInt(maxCounters);
        counter = new HyperLolLolCounterArray(arraySize, n, log2m);
    }

    private void assertAllCountersLargerThanZero(int arraySize) {
        for(int i = 0; i < arraySize ; i++) {
            assertTrue(counter.count(i) > 0);
        }
    }

    private void randomlyAddHashesToCounters(int numberOfCounters) {
        for (int i = 0; i < numberOfCounters; i++) {
            int maxAddedValues = 100;
            for(int j = 0; j < maxAddedValues; j++) {
                int addedValue = rand.nextInt();
                counter.add(i, addedValue);
            }
        }
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

