import org.junit.Test;
import static org.junit.Assert.*;
import se.meltwater.GraphChanger;

/**
 * Created by simon on 2016-02-26.
 */
public class TestIntervalMerge extends GraphChanger {

    @Test
    public void testIntervalOverlap(){


        for(Intervals test : getIntervals()){
            assertEquals(test.shouldOverlap,intervalsCanBeJoint(test.interval1,test.interval2));
        }

    }

    @Test
    public void testIntervalResult(){
        for(Intervals test : getIntervals()){
            long[] result = test.interval1;
            assertEquals(test.shouldOverlap, tryToJoinIntervals(result,test.interval2));
            if(test.shouldOverlap) {
                assertArrayEquals(test.resultInterval, result);
            }else {
                assertArrayEquals(test.interval1, result);
            }
        }
    }

    @Test
    public void testInInterval(){
        assertTrue(isInIntervals(1,new long[][]{new long[]{1,2}}));
        assertFalse(isInIntervals(2,new long[][]{new long[]{1,2}}));
        assertTrue(isInIntervals(1,new long[][]{new long[]{1,2},new long[]{4,5}}));
        assertFalse(isInIntervals(0,new long[][]{new long[]{1,2}}));
        assertTrue(isInIntervals(8,new long[][]{new long[]{4,15}}));
        assertTrue(isInIntervals(8,new long[][]{new long[]{4,15}, new long[]{0,0}}));
        assertFalse(isInIntervals(3,new long[][]{new long[]{4,15}, new long[]{0,0}}));
    }

    @Test
    public void testIntervalsMerge(){
        long[][] intervals1 = new long[][]{new long[]{1,2},new long[]{3,4}, new long[]{6,8}};
        long[][] intervals2 = new long[][]{new long[]{4,6}};
        long[][] result = new long[4][2];
        Pair<Integer,Integer> res = mergeIntervals(intervals1,intervals2,result);
        assertEquals((int)res.fst,2);
        assertEquals((int)res.snd,6); // 1,3-8
        assertArrayEquals(result[0],new long[]{1,2});
        assertArrayEquals(result[1],new long[]{3,8});

        intervals1 = new long[][]{new long[]{1,2}, new long[]{7,8}, new long[]{9,10}};
        intervals2 = new long[][]{new long[]{4,6}};
        result = new long[4][2];
        res = mergeIntervals(intervals1,intervals2,result);
        assertEquals((int)res.fst,4);
        assertEquals((int)res.snd,5); // 1,4-6,7,9
        assertArrayEquals(result[0],new long[]{1,2});
        assertArrayEquals(result[1],new long[]{4,6});
        assertArrayEquals(result[2],new long[]{7,8});
        assertArrayEquals(result[3],new long[]{9,10});
    }

    private static Intervals[] getIntervals(){
        Intervals[] tests = {new Intervals(1,2,3,4),
                            new Intervals(1,2,2,4,1,4),
                            new Intervals(1,2,4,5),
                            new Intervals(1,4,3,6,1,6),
                            new Intervals(4,5,1,2)};

        return tests;
    }

    static class Intervals{
        public long[] interval1,interval2;
        public boolean shouldOverlap;
        public long[] resultInterval;

        public Intervals(long start1, long end1, long start2, long end2){
            interval1 = new long[]{start1,end1};
            interval2 = new long[]{start2,end2};
            this.shouldOverlap = false;

        }

        public Intervals(long start1, long end1, long start2, long end2, long mergedStart, long mergedEnd){
            this(start1,end1,start2,end2);
            resultInterval = new long[]{mergedStart,mergedEnd};
            this.shouldOverlap = true;
        }

    }

}
