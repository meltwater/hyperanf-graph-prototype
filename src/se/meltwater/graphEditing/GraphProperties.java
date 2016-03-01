package se.meltwater.graphEditing;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by simon on 2016-03-01.
 */
public class GraphProperties extends Properties {

    private long numNodes;
    private byte zetak;
    private int intervalLength;
    private int windowsize;
    private String compressionFlags;

    public GraphProperties(String file) throws IOException {
        InputStream propF = new FileInputStream(file);
        Properties prop = new Properties();
        prop.load(propF);
        numNodes = Long.parseLong(prop.getProperty("nodes"));
        zetak = Byte.parseByte(prop.getProperty("zetak"));
        intervalLength = Integer.parseInt(prop.getProperty("minintervallength"));
        windowsize = Integer.parseInt(prop.getProperty("windowsize"));
        compressionFlags = prop.getProperty("compressionflags");
        propF.close();
    }

    public long getNumNodes() {
        return numNodes;
    }

    public byte getZetak() {
        return zetak;
    }

    public int getIntervalLength() {
        return intervalLength;
    }

    public int getWindowsize() {
        return windowsize;
    }

    public String getCompressionFlags() {
        return compressionFlags;
    }
}
