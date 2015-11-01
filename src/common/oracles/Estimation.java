package common.oracles;

/**
 * Created by semionn on 28.10.15.
 */
public class Estimation {
    private int recordCount;
    private int pagesCount;
    private int cost;

    public Estimation(int recordCount, int pagesCount, int cost) {
        this.recordCount = recordCount;
        this.pagesCount = pagesCount;
        this.cost = cost;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    public int getPagesCount() {
        return pagesCount;
    }

    public void setPagesCount(int pagesCount) {
        this.pagesCount = pagesCount;
    }

    public int getCost() {
        return cost;
    }

    public void setCost(int cost) {
        this.cost = cost;
    }
}
