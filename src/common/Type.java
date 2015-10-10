package common;

import java.util.Map;

/**
 * Created by semionn on 09.10.15.
 */
public class Type {

    BaseType baseType;
    Map<String, Object> params;

    Type(BaseType baseType, Map<String, Object> params){
        this.baseType = baseType;
        this.params = params;
    }

    Type(BaseType baseType)
    {
        this(baseType, null);
    }

}
