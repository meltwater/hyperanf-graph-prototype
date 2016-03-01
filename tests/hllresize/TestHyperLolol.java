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

    @Test
    public void testResize() throws IOException {
        int iteration = 0;
        while(iteration++ < 100) {
            final int maxSize = 100;
            final int log2mMinSize = 4;
            Random rand = new Random();
            int arraySize = rand.nextInt(maxSize);
            int n = maxSize * 2;
            int log2m = rand.nextInt(10) + log2mMinSize;

            HyperLolLolCounterArray counter = new HyperLolLolCounterArray(arraySize, n, log2m);

            for (int i = 0; i < arraySize; i++) {
                int maxAddedValues = 100;
                for(int j = 0; j < maxAddedValues; j++) {
                    int addedValue = rand.nextInt();
                    counter.add(i, addedValue);
                }
            }

            double prevCounts[] = new double[arraySize];
            for (int i = 0; i < arraySize; i++) {
                prevCounts[i] = counter.count(i);
            }

            final int increaseSize = rand.nextInt(maxSize);
            counter.increaseCounterSize(increaseSize);

            //All previous values are still present
            for (int i = 0; i < arraySize; i++) {
                assertTrue(prevCounts[i] == counter.count(i));
            }

            //All new values are 0
            for(int i = arraySize; i < arraySize + increaseSize; i++) {
                assertTrue(counter.count(i) == 0);
            }
        }
    }
}
