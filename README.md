Description
-----------

Loads the content of a list of files into a table of format:

   * TableName: 'files'
   * RowKey: filename
   * ColumnFamilty: 'content:'

Compile
-------

$ mvn clean install

Run
---

$ mvn exec:java -Dexec.mainClass=com.igalia.hbaseloader.HBaseLoader -Dtablename=files -Ddir=dir

Parameters
----------

   * <tablename>. Name of the table where to import the files.
   * <dir>. Directory containing files to import.
