import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.spi.ToolProvider;

/**
 * Class which needs to be implemented.  ONLY this class should be modified
 */
public class DatabaseDumper201 extends DatabaseDumper {

    /**
     * 
     * @param c connection which the dumper should use
     * @param type a string naming the type of database being connected to e.g. sqlite
     */
    public DatabaseDumper201(Connection c,String type) {
        super(c,type);
    }
    /**
     * 
     * @param c connection to a database which will have a sql dump create for
     */
    public DatabaseDumper201(Connection c) {
        super(c,c.getClass().getCanonicalName());
    }

    public List<String> getTableNames() {
        List<String> result = new ArrayList<>();

        try{
            DatabaseMetaData databaseMetaData = getConnection().getMetaData();
            String[] types = {"TABLE"};
            ResultSet resultSet = databaseMetaData.getTables(null, null, null, types);

            while( resultSet.next() )
            {
                String tableName = resultSet.getString("TABLE_NAME");
                result.add(tableName);
            }
        }catch(SQLException se){
            System.out.println(String.format("Exception caught in getTableNames() - (%s)\n", se));
        }
        return result;
    }

    private void closedResultSetTest(ResultSet rs){
        try {
            if(rs.isClosed()){
                System.out.println("resultSet is closed...");
            }
            else{
                System.out.println("resultSet is open...");
            }
        } catch (SQLException ex) {
            System.out.println(String.format("Exception caught in closedResultSetTest() - (%s)\n", ex));
        }
    }

    @Override
    public List<String> getViewNames() {

        List<String> result = new ArrayList<>();

        try{
            DatabaseMetaData databaseMetaData = getConnection().getMetaData();
            String[] types = {"VIEW"};
            ResultSet resultSet = databaseMetaData.getTables(null, null, null, types);

            while( resultSet.next() )
            {
                String tableName = resultSet.getString("TABLE_NAME");
                
                System.out.println("tableName:    "+ tableName);
                result.add(tableName);
            }
        }catch(SQLException se){
            System.out.println(String.format("Exception caught in getViewNames() - (%s)\n", se));
        }
        return result;
    }

    @Override
    public String getDDLForTable(String tableName) {

        String createTableStatement = String.format("DROP TABLE IF EXISTS %s;\n CREATE TABLE %s (\n", quotify(tableName), doubleQuotify(tableName));

        try{
            DatabaseMetaData metaData = getConnection().getMetaData();

            List<String> attributes = new ArrayList<>();
            List<String> primaryKeys = new ArrayList<>();
            List<String> importedKeys = new ArrayList<>();

            ResultSet columnsRS = metaData.getColumns(null, null, tableName, null);
            ResultSet primaryKeysRS = metaData.getPrimaryKeys(null, null, tableName);
            ResultSet foreignKeysRS = metaData.getImportedKeys(null, null, tableName);

            //add Attributes to the attribute ArrayList
            while(columnsRS.next()){

                String attribute = String.format("    %s   %s", doubleQuotify(columnsRS.getString("COLUMN_NAME")), columnsRS.getString("TYPE_NAME"));

                if( columnsRS.getString("IS_NULLABLE") == "N" ){
                    attribute += String.format("   NOT NULL,\n");
                }
                attributes.add(attribute);
            }

            //add primary key(s) to the primary key ArrayList
            while(primaryKeysRS.next()){
                primaryKeys.add(doubleQuotify(primaryKeysRS.getString("COLUMN_NAME")));
            }
            String primaryKeysLine = String.format("    PRIMARY KEY (%s)", String.join(",", primaryKeys));

            //add foreign key(s) to the foreign key ArrayList
            while(foreignKeysRS.next()){
                importedKeys.add(String.format("    FOREIGN KEY (%s) REFERENCES %s(%s) ", doubleQuotify(foreignKeysRS.getString("FKCOLUMN_NAME")), doubleQuotify(foreignKeysRS.getString("PKTABLE_NAME")), doubleQuotify(foreignKeysRS.getString("FKCOLUMN_NAME"))));
            }

            //concatenate all of the create table statements
            attributes.add(primaryKeysLine);
            attributes.addAll(importedKeys);

            createTableStatement += String.join(",\n", attributes);

            createTableStatement += "\n);\n";

        }catch(SQLException e){
            System.out.println(String.format("Exception caught in getDDLForTable() - (%s)\n", e));
        }

        return createTableStatement;
    }

    @Override
    public String getInsertsForTable(String tableName) {
        
        List<String> attributeNames = new ArrayList<>();
        int[] attributeTypes;

        List<String> insertStatements = new ArrayList<>();
        int columnCount;

        try{
            String selectStatement = String.format("SELECT * FROM %s", tableName);
            PreparedStatement statement = getConnection().prepareStatement(selectStatement);
            ResultSet selectRS = statement.executeQuery();
            ResultSetMetaData selectRSmeta = selectRS.getMetaData();

            //get column count
            columnCount = selectRSmeta.getColumnCount();

            // get the attribute names
            attributeNames = columnNameFinder(columnCount, selectRSmeta);

            // get the column types
            attributeTypes = columnTypeFinder(columnCount, selectRSmeta);
            
            while(selectRS.next()){
                StringBuffer currentInsert = new StringBuffer(String.format("INSERT INTO %s( ", tableName));
                List<String> data = new ArrayList<>();
                for(int i = 1; i <= columnCount; i++){

                    switch(attributeTypes[i-1]){
                        case -1:
                        case 1:
                        case 12:
                            data.add(doubleQuotify(selectRS.getString(i)));
                            break;
                        case 2:
                        case 3:
                        case 4:
                            data.add(Integer.toString(selectRS.getInt(i)));
                            break;
                        case 6:
                            data.add(Float.toString(selectRS.getFloat(i)));
                            break;
                        case 0:
                            data.add("NULL");
                            break;
                    }
                }
                currentInsert.append(String.format("%s ) VALUES ( %s );", String.join(",", attributeNames), String.join(",",data)));
                insertStatements.add(currentInsert.toString());
            }
        }catch(SQLException ex){
            System.out.println(String.format("Exception caught in getInsertsForTable() - (%s)\n", ex));
        }
        return String.join("\n", insertStatements);
    }

    /**
     * builds an array of integers representing the column tyoes
     * 
     * @param count the number of the columns in the ResultSetMetaData
     * @param selectMetaData the object storing metadata about the ResultSet
     * @return names the array of integers containing all integer column types for the ResultSet
     */
    public int[] columnTypeFinder(int count,  ResultSetMetaData selectMetaData){

        int[] types = new int[count];

        for(int i = 1; i <= count; i++){
            try{
                types[i-1] = selectMetaData.getColumnType(i);
            }catch(SQLException ex){
                System.out.println(String.format("Exception caught in columnTypeFinder() - (%s)\n", ex));
            }
        }
        return types;
    }

    /**
     * adds all column names from a ResultSet to an ArrayList
     * 
     * @param count the number of the columns in the ResultSetMetaData
     * @param selectMetaData the object storing metadata about the ResultSet
     * @return names the ArrayList of Strings containing all the column names for the ResultSet
     */
    public List<String> columnNameFinder(int count, ResultSetMetaData selectMetaData){
        List<String> names = new ArrayList<>();
        try{
            for(int i = 1; i <= count; i++) names.add(quotify(selectMetaData.getColumnName(i)));
        }        
        catch(SQLException ex){
            System.out.println(String.format("Exception caught in columnNameFinder() - (%s)\n", ex));
        }
        return names;
    }

    /**
     * adds single quotation marks around a String
     * 
     * @param unquoted the String which will have quotes put around it
     * @return the quoted String
     */
    public String quotify(String unquoted){

        return String.format("'%s'", unquoted);
    }

    /**
     * adds double quotation marks around a String
     * 
     * @param unquoted the String which will have double quotes put around it
     * @return the quoted String
     */
    public String doubleQuotify(String unquoted){

        return String.format("\"%s\"", unquoted);
    }

    @Override
    public String getDDLForView(String viewName) {
        String createViewStatement = String.format("CREATE TABLE %s (\n", doubleQuotify("view_"+ viewName));

        try{
            DatabaseMetaData metaData = getConnection().getMetaData();

            List<String> attributes = new ArrayList<>();
            ResultSet columnsRS = metaData.getColumns(null, null, viewName, null);

            //add Attributes to the attribute ArrayList
            while(columnsRS.next()){

                String attribute = String.format("    %s   %s", doubleQuotify(columnsRS.getString("COLUMN_NAME")), columnsRS.getString("TYPE_NAME"));

                if( columnsRS.getString("IS_NULLABLE") == "N" ){
                    attribute += String.format("   NOT NULL,\n");
                }
                attributes.add(attribute);
            }

            //concatenate all of the create table statemtent
            createViewStatement += String.join(",\n", attributes);
            createViewStatement += "\n);\n";
        }
        catch(SQLException e){
            System.out.println(String.format("Exception caught in getDDLForView() - (%s)\n", e));
        }
        return createViewStatement;
    }



    @Override
    public String getDumpString() {
        List<String> tableNames = getTableNames();
        List<String> viewNames = getViewNames();
        StringBuffer dumpedString = new StringBuffer();

        for(String table: getTableNames()){

            dumpedString.append(getDDLForTable(table));
            dumpedString.append("\n-- -- --\n");
            dumpedString.append(getInsertsForTable(table));
            dumpedString.append("\n-- -- --\n");
        }
        for(String view: viewNames){

            dumpedString.append(getDDLForView(view));
            dumpedString.append("\n-- -- --\n");
            dumpedString.append(getInsertsForView(view));
            dumpedString.append("\n-- -- --\n");
        }
        dumpedString.append(getDatabaseIndexes());

        return dumpedString.toString();
    }

    @Override
    public void dumpToFileName(String fileName) {
        
        if(getDumpString() != null){
            try{
                FileWriter file = new FileWriter(fileName);
                file.write(getDumpString());
            }catch(IOException e){
                e.printStackTrace();
            }
        }else System.out.println("NULL string cannot be dumped");
    }

    @Override
    public void dumpToSystemOut() {
        getDumpString();
        System.out.println(getDumpString());
    }


    public String getInsertsForView(String viewName) {
        
        List<String> attributeNames = new ArrayList<>();
        int[] attributeTypes;

        List<String> insertStatements = new ArrayList<>();
        int columnCount;

        try{
            String selectStatement = String.format("SELECT * FROM %s", viewName);
            PreparedStatement statement = getConnection().prepareStatement(selectStatement);
            ResultSet selectRS = statement.executeQuery();
            ResultSetMetaData selectRSmeta = selectRS.getMetaData();

            //get column count
            columnCount = selectRSmeta.getColumnCount();

            // get the attribute names
            attributeNames = columnNameFinder(columnCount, selectRSmeta);

            // get the column types
            attributeTypes = columnTypeFinder(columnCount, selectRSmeta);

            while(selectRS.next()){
                StringBuffer currentInsert = new StringBuffer(String.format("INSERT INTO %s(\n ", doubleQuotify("view_"+ viewName)));
                List<String> data = new ArrayList<>();
                for(int i = 1; i <= columnCount; i++){

                    switch(attributeTypes[i-1]){
                        case -1:
                        case 1:
                        case 12:
                            data.add(quotify(selectRS.getString(i)));
                            break;
                        case 2:
                        case 3:
                        case 4:
                            data.add(Integer.toString(selectRS.getInt(i)));
                            break;
                        case 6:
                            data.add(Float.toString(selectRS.getFloat(i)));
                            break;
                        case 0:
                            data.add(""+ quotify("NULL"));
                            break;
                    }
                }
                currentInsert.append(String.format("%s \n)\n VALUES ( %s );", String.join(",\n", attributeNames), String.join(",",data)));
                insertStatements.add(currentInsert.toString());
            }
        }catch(SQLException ex){
            System.out.println(String.format("Exception caught in getInsertsForView() - (%s)\n", ex));
        }
        System.out.println(String.join("\n", insertStatements));
        return String.join("\n", insertStatements);
    }

    @Override
    public String getDatabaseIndexes() {
        List<String> columns = new ArrayList<>();
        try{
            DatabaseMetaData metaData = getConnection().getMetaData();

            for(String table: getTableNames()){
                // String createcreateViewStatement = String.format("CREATE INDEX %s (\n", quotify(table), doubleQuotify(table));
                ResultSet columnsRS = metaData.getIndexInfo(null, null, table, false, false);

                while(columnsRS.next()){
                    StringBuffer createIndexStatement = new StringBuffer("CREATE ");
                    String order = columnsRS.getString("ASC_OR_DESC");

                    if(!columnsRS.getBoolean("NON_UNIQUE")){
                        createIndexStatement.append("UNIQUE ");
                    }
                    createIndexStatement.append(String.format("%s ON %s(%s)", doubleQuotify(columnsRS.getString("INDEX_NAME")), doubleQuotify(columnsRS.getString("TABLE_NAME")), doubleQuotify(columnsRS.getString("COLUMN_NAME"))));

                    if(order != null){
                        String orderString = "ORDER BY ";
                        if(order == "A"){
                            createIndexStatement.append(orderString + "ASC;");
                        }
                        else if(order =="D"){
                            createIndexStatement.append(orderString + "DESC;");
                        }
                        else System.out.println("error in getString(\"ASC_OR_DESC\")");
                    }
                    columns.add(createIndexStatement.toString());
                }
            }
        }
        catch(SQLException se){
            System.out.println(String.format("Exception caught in columnNameFinder() - (%s)\n", se));
        }
        return String.join("\n", columns);
    }
}