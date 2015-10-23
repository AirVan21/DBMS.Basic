package common.table_classes;

import common.Column;

import java.util.List;
import java.io.Serializable;

/**
 * Created by airvan21 on 17.10.15.
 */
public class MetaPage implements  Serializable {

    Integer recordLength;
    Integer firstPageWithSpace;
    Integer firstPageWithNoSpace;

    public MetaPage(List<Column> columns){
        this.recordLength = 0;
        this.firstPageWithSpace = 0;
        this.firstPageWithNoSpace = 0;
    }
}
