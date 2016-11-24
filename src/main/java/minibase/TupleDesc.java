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

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TupleDesc other = (TupleDesc) obj;
        if (tdItems == null) {
            if (other.tdItems != null)
                return false;
        } else {
            int nItems = tdItems.size();
            if (other.tdItems.size() != nItems)
                return false;
            for (int i = 0; i < nItems; i++) {
                if (tdItems.get(i).fieldType != other.tdItems.get(i).fieldType)
                    return false;
            }
        }
        return true;
    }



}
