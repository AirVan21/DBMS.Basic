package parser;

import common.Statement;
import common.exceptions.QueryException;

/**
 * Created by semionn on 27.10.15.
 */
public interface ISQLParser {
    Statement parse(String query) throws QueryException;
}
