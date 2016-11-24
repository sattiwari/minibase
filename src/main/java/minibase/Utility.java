package minibase;

import java.util.ArrayList;

/**
 * Created by stiwari on 11/24/2016 AD.
 */
public class Utility {

    public static Type[] getTypes(int n) {
        Type[] types = new Type[n];
        for (int i = 0; i < n; i++) {
            types[i] = Type.INT_TYPE;
        }
        return types;
    }

    public static TupleDesc getTupleDesc(int n) {
        return new TupleDesc(getTypes(n));
    }

}
