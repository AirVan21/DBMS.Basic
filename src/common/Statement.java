package common;

import java.util.Map;
import java.util.Objects;

/**
 * Created by semionn on 27.10.15.
 */
public class Statement {
    final StatementType type;
    final Map<String, Object> params;

    public Statement(StatementType type, Map<String, Object> params) {
        this.type = type;
        this.params = params;
    }

    public String getStringParam(String paramName) {
        return (String) params.get(paramName);
    }

    public Object getParam(String paramName) {
        return params.get(paramName);
    }

    public StatementType getType() {
        return type;
    }
}
