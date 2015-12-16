package common.exceptions;

/**
 * Created by semionn on 16.12.15.
 */
public class ReadPageException extends Exception {
    public ReadPageException() { super(); }
    public ReadPageException(String message) { super(message); }

    @Override
    public String getMessage() {
        return super.getMessage();
    }
}
