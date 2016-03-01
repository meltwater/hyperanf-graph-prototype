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

    @Test
    public void testResize() throws IOException {
        int iteration = 0;
        while(iteration++ < nrTestIterations) {
            Random rand = new Random();
            int arraySize = rand.nextInt(maxCounters);
            int n = maxCounters * 2;
            int log2m = rand.nextInt(10) + log2mMinSize;

            HyperLolLolCounterArray counter = new HyperLolLolCounterArray(arraySize, n, log2m);

            randomlyAddHashesToCounters(counter, arraySize);

            double[] prevCounts = getCurrentCountersAsList(counter, arraySize);

            final int increaseSize = rand.nextInt(maxCounters);
            counter.increaseCounterSize(increaseSize);

            assertPreviousValuesAreIntact(counter, arraySize, prevCounts);
            assertNewValuesAreZero(counter, arraySize, increaseSize);
        }
    }

    private void randomlyAddHashesToCounters(HyperLolLolCounterArray counter, int arraySize) {
        Random rand = new Random();
        for (int i = 0; i < arraySize; i++) {
            int maxAddedValues = 100;
            for(int j = 0; j < maxAddedValues; j++) {
                int addedValue = rand.nextInt();
                counter.add(i, addedValue);
            }
        }
    }

    private double[] getCurrentCountersAsList(HyperLolLolCounterArray counter, int arraySize) {
        double prevCounts[] = new double[arraySize];
        for (int i = 0; i < arraySize; i++) {
            prevCounts[i] = counter.count(i);
        }

        return prevCounts;
    }

    private void assertPreviousValuesAreIntact(HyperLolLolCounterArray counter, int arraySize, double[] prevCounts) {
        for (int i = 0; i < arraySize; i++) {
            assertTrue(prevCounts[i] == counter.count(i));
        }
    }

    private void assertNewValuesAreZero(HyperLolLolCounterArray counter, int arraySize, int increaseSize) {
        for(int i = arraySize; i < arraySize + increaseSize; i++) {
            assertTrue(counter.count(i) == 0);
        }
    }
}

