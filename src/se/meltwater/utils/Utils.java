package se.meltwater.utils;

import com.javamex.classmexer.MemoryUtil;

import java.util.ArrayList;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class Utils {

    /**
     *
     * @param elem
     * @param times
     * @param dummyArr This is for {@link ArrayList#toArray(Object[])} which needs a dummy array
     * @param <T>
     * @return
     */
    public static <T> T[] repeat(T elem, int times, T[] dummyArr){
        ArrayList<T> ret = new ArrayList<>(times);
        for (int i = 0; i < times ; i++) {
            ret.add(elem);
        }
        return ret.toArray(dummyArr);
    }



    public static long getMemoryUsage(Object obj) {
        return MemoryUtil.deepMemoryUsageOf(obj, MemoryUtil.VisibilityFilter.ALL);
    }
}
