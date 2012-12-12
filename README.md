Description
-----------

Loads the content of a list of files into a table of format (table;key;:content)

Create table
------------

Within a HBase shell session:

$ create table 'files','content'

Compile
-------

$ mvn clean install

Run
---

$ mvn exec:java -Dexec.mainClass=com.igalia.hbaseloader.HBaseLoader

Parameters
----------

   * <tablename>. Name of the table where to import the files.
   * <dir>. Directory containing files to import.
