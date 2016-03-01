package se.meltwater.history;

/**
 * Created by johan on 2016-03-01.
 */
public class NodeHistory implements INodeHistory {
        
    @Override
    public void createEmptyHistory(long node) {

    }

    @Override
    public void updateHistory(long node, int h, int register, int value) {

    }

    @Override
    public int[] getHistoryLevel(long node, int h) {
        return new int[0];
    }
}
