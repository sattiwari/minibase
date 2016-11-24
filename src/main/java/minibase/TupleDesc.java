package minibase;

import java.util.ArrayList;
import java.util.List;

public class TupleDesc {

    public static class TDItem {
        public final Type fieldType;
        public final String fieldName;

        public TDItem(Type type, String name) {
            this.fieldType = type;
            this.fieldName = name;
        }

        public static List<TDItem> getListFrom(Type[] typeArr, String[] nameArr) {
            List<TDItem> items = new ArrayList<TDItem>();
            for(int i = 0; i < typeArr.length; i++) {
                items.add(new TDItem(typeArr[i], nameArr[i]));
            }
            return items;
        }
    }

    private final List<TDItem> tdItems;

    public TupleDesc(Type[] typeArr, String[] nameArr) {
        this.tdItems = TDItem.getListFrom(typeArr, nameArr);
    }

    public TupleDesc(Type[] typeArr) {
        this(typeArr, new String[typeArr.length]);
    }



}
