package common.conditions;

import common.exceptions.QueryException;
import parser.sql_antlr.SQLiteParser;

/**
 * Created by semionn on 31.10.15.
 */
public enum ComparisonType {
    LESS("<"),
    LESSEQ("<="),
    EQUAL("="),
    GREATEQ(">="),
    GREAT(">");

    String symbols;

    ComparisonType(String symbols) {
        this.symbols = symbols;
    }

    public static ComparisonType fromString(String comparison) {
        for (ComparisonType comparisonType : ComparisonType.values()) {
            if (comparisonType.symbols.equals(comparison))
                return comparisonType;
        }
        throw new IllegalArgumentException(String.format("Not found comparison '%s'", comparison));
    }

}
