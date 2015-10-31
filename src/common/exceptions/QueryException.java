package common.exceptions;

/**
 * Created by semionn on 31.10.15.
 */
public class QueryException extends Exception {
    public QueryException() { super(); }
    public QueryException(String message) { super(message); }

    @Override
    public String getMessage() {
        return super.getMessage();
    }
}
