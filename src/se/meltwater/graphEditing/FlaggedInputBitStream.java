package se.meltwater.graphEditing;

import it.unimi.dsi.io.InputBitStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Simon Lindhén
 * @author Johan Nilsson Hansen
 *
 * // TODO class description
 */
public class FlaggedInputBitStream extends InputBitStream {

    private byte offsetEncoding = GAMMAC;
    private byte residualEncoding = ZETAC;
    private byte outDegreeEncoding = GAMMAC;
    private final static byte GAMMAC=0, ZETAC = 1, DELTAC = 2, GOLOMBC = 3, NIBBLEC = 4;
    private byte zetaK;

    public FlaggedInputBitStream(InputStream in, String flags, byte zetaK){
        super(in);
        setFlags(flags);
        this.zetaK = zetaK;
    }

    private void setFlags(String flagstring){

        if(flagstring.length() == 0)
            return;

        String[] flags = flagstring.toLowerCase().split("\\|");
        for(String flag : flags){
            flag = flag.trim();
            if(flag.startsWith("outdegrees_")){
                if(flag.endsWith("gamma"))
                    outDegreeEncoding = GAMMAC;
                else if(flag.endsWith("delta"))
                    outDegreeEncoding = DELTAC;
            }else if(flag.startsWith("residuals")){
                if(flag.endsWith("gamma"))
                    residualEncoding = GAMMAC;
                else if(flag.endsWith("zeta"))
                    residualEncoding = ZETAC;
                else if(flag.endsWith("delta"))
                    residualEncoding = DELTAC;
                else if(flag.endsWith("nibble"))
                    residualEncoding = NIBBLEC;
                else if(flag.endsWith("golomb"))
                    residualEncoding = GOLOMBC;
            }else if(flag.startsWith("offsets")){
                if(flag.endsWith("gamma"))
                    offsetEncoding = GAMMAC;
                else if(flag.endsWith("delta"))
                    offsetEncoding = DELTAC;
            }

        }
    }

    public long readOffset() throws IOException {
        switch (offsetEncoding){
            case GAMMAC:
                return readLongGamma();
            case DELTAC:
                return readLongDelta();
        }
        throw new IllegalStateException("No valid offset encoding: " + offsetEncoding);
    }

    public int readOutdegree() throws IOException {
        switch (outDegreeEncoding){
            case GAMMAC:
                return readGamma();
            case DELTAC:
                return readDelta();
        }
        throw new IllegalStateException("No valid outdegree encoding: " + offsetEncoding);
    }

    public long readResidual() throws IOException {
        switch (residualEncoding){
            case GAMMAC:
                return readLongGamma();
            case DELTAC:
                return readLongDelta();
            case GOLOMBC:
                return readLongGolomb(zetaK);
            case NIBBLEC:
                return readLongNibble();
            case ZETAC:
                return readLongZeta(zetaK);
        }
        throw new IllegalStateException("No valid offset encoding: " + offsetEncoding);
    }

}
