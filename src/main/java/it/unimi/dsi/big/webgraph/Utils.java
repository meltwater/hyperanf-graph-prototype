package it.unimi.dsi.big.webgraph;

import com.javamex.classmexer.MemoryUtil;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 **/
public class Utils {

    public static long getMemoryUsage(Object ... obj) {
        return MemoryUtil.deepMemoryUsageOf(obj, MemoryUtil.VisibilityFilter.ALL);
    }

}
