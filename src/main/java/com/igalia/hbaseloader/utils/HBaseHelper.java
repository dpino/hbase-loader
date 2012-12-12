package com.igalia.hbaseloader.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author Diego Pino García <dpino@igalia.com>
 * 
 */
public class HBaseHelper {

	private static final Configuration conf = HBaseConfiguration.create();
	private static final HTablePool tablePool = new HTablePool(conf, Integer.MAX_VALUE);

	public static HBaseHelper create() {
		HBaseHelper result = new HBaseHelper();
		try {
			result.hbase = new HBaseAdmin(conf);
			return result;
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
		return null;
	}

	private HBaseAdmin hbase;

	static {
		conf.set("hbase.master", "localhost:60000");
	}

	private HBaseHelper() {

	}	

	public HTable createTable(String tableName, String... descriptors)
			throws IOException {
		if (tableExists(tableName)) {
			dropTable(tableName);
		}
		return doCreateTable(tableName, descriptors);
	}

	private HTable doCreateTable(String tableName, String... descriptors)
			throws IOException {
		HTableDescriptor descriptor = new HTableDescriptor(tableName);
		for (String each : descriptors) {
			HColumnDescriptor cd = new HColumnDescriptor(each.getBytes());
			descriptor.addFamily(cd);
		}
		hbase.createTable(descriptor);
		debug(String.format("Database %s created", tableName));
		return new HTable(tableName);
	}
	
    private static void debug(Object obj) {
        System.out.println(String.format("### DEBUG: %s", obj.toString()));
    }    

	public void dropTable(String tableName) throws IOException {
		hbase.disableTable(tableName);
		hbase.deleteTable(tableName);
	}

	public HTable getOrCreateTable(String tableName, String... descriptors)
			throws IOException {
		if (!tableExists(tableName)) {
			doCreateTable(tableName, descriptors);
		}
		return getTable(tableName);
	}
	
	public HTable getTable(String tableName) {
		return (HTable) tablePool.getTable(tableName);
	}

	public void insert(HTable table, String rowKey, List<String> values)
			throws IOException {
		if (values.size() == 3) {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(values.get(0)), Bytes.toBytes(values.get(1)),
					Bytes.toBytes(values.get(2)));
			table.put(put);
		}
	}

	public void insert(HTable table, String rowKey, String prefix, String qualifier, byte[] value)
			throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(prefix), Bytes.toBytes(qualifier), value);
		table.put(put);
	}
	
	public boolean tableExists(String tableName) {
		try {
			return hbase.tableExists(tableName);
		} catch (IOException e) {			
			e.printStackTrace();
		}
		return false;
	}

}