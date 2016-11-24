package minibase;

/**
 * Created by stiwari on 11/24/2016 AD.
 */
public class Database {

    private static Database _instance = new Database();
    private final Catalog _catalog;

    private Database() {
        _catalog = new Catalog();
    }

    public static Catalog getCatalog() {
        return _instance._catalog;
    }
}
