package minibase;

import java.util.*;

/**
 * Created by stiwari on 11/23/2016 AD.
 */
public class Catalog {

    private final List<DbFile> files;
    private final List<String> tableNames;
//    private final List<String> primaryKeyFields;
    private final Map<String, Integer> nameToIdMap;
    private final Map<Integer, Integer> idToIndexMap;

    public Catalog() {
        this.files = new ArrayList<DbFile>();
        this.tableNames = new ArrayList<String>();
//        this.primaryKeyFields = new ArrayList<String>();
        this.nameToIdMap = new HashMap<String, Integer>();
        this.idToIndexMap = new HashMap<Integer, Integer>();
    }

    public void addTable(DbFile file, String tableName, String pkField) {
        Integer id = new Integer(file.getId());
        nameToIdMap.put(tableName, id);
        idToIndexMap.put(id, new Integer(files.size()));
        this.files.add(file);
        this.tableNames.add(tableName);
//        this.primaryKeyFields.add(pkField);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    public int getTableId(String tableName) throws NoSuchElementException {
        if(nameToIdMap.containsKey(tableName)) {
            return nameToIdMap.get(tableName);
        }
        throw new NoSuchElementException();
    }

    private void checkId(int tableId) throws NoSuchElementException {
        if(!idToIndexMap.containsKey(new Integer(tableId)))
            throw new NoSuchElementException();
    }

    public int getIndex(int tableId) throws NoSuchElementException {
        checkId(tableId);
        return idToIndexMap.get(new Integer(tableId)).intValue();
    }

    public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
        return files.get(idToIndexMap.get(new Integer(tableId))).getTupleDesc();
    }

    public DbFile getDatabaseFile(int tableId) throws NoSuchElementException {
        return files.get(getIndex(tableId));
    }



}
