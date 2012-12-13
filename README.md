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

   * &lt;tablename&gt;: Name of the table where to import the files.
   * &lt;dir&gt;: Directory containing files to import.
