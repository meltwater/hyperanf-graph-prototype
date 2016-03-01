package se.meltwater.history;

/**
 * Created by johan on 2016-03-01.
 */
public interface INodeHistory {
    public void createEmptyHistory(long node);

    public void updateHistory(long node, int h, int register, int value);

    public int[] getHistoryLevel(long node, int h);

}
