package it.unimi.dsi.big.webgraph;

import com.javamex.classmexer.MemoryUtil;

import java.util.ArrayList;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *         <p>
 *         // TODO Class description
 */
public class Utils {

    public static long getMemoryUsage(Object ... obj) {
        return MemoryUtil.deepMemoryUsageOf(obj, MemoryUtil.VisibilityFilter.ALL);
    }

}
