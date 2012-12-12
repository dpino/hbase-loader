package com.igalia.hbaseloader;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import com.igalia.hbaseloader.utils.HBaseHelper;

/**
 * Loads files into a table:
 * 	* Tablename: 'files'
 * 	* ColumnFamiy: 'content:'
 * 
 * Use:
 * 	HBaseLoader <tableName> <dir>
 * 
 * @params tableName Name of the table where to import files
 * @params dir Directory containing files to import
 * 
 * Diego Pino García <dpino@igalia.com>
 *
 */
public class HBaseLoader {

	private static final HBaseHelper hbase =  HBaseHelper.create();
	
    public static void main(String[] args) {    	
    	if (args.length < 1 || args.length > 2) {
    		error("HBaseLoader <tablename> <dir>");
    		return;
    	}
		if (hbase == null) {
			error("Couldn't establish connection to HBase");
		}

		String fileName = args[1];
		File file = new File(fileName);
		if (file == null) {
			error(String.format("Directory %s doesn't exist", fileName));
		}
		if (!file.isDirectory()) {
			error(String.format("File %s is not a directory", fileName));
		}
		
		HTable table;
		try {
			String tableName = args[0];
			table = hbase.getOrCreateTable(tableName, "content");
			for (File each: file.listFiles()) {
				if (each.isFile()) {
					insertInto(table, each);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
    }

	private static void insertInto(HTable table, File file) {
		try {
			byte[] content = Bytes.toBytes(FileUtils.readFileToString(file, "UTF-8"));
			hbase.insert(table, file.getName(), "content", "", content);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void error(String str) {
		System.out.println("Error: " + str);
	}
    
}