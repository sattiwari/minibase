package minibase;

/**
 * Created by stiwari on 11/24/2016 AD.
 */
public class TestUtil {

    public static class SkeltonFile implements DbFile {
        private int tableId;
        private TupleDesc td;

        public SkeltonFile(int tableId, TupleDesc td) {
            this.tableId = tableId;
            this.td = td;
        }

        public int getId() {
            return tableId;
        }

        public TupleDesc getTupleDesc() {
            return td;
        }
    }

}
