/*******************************************************************************
* Copyright (c) 2013 Vladimir Rodionov. All Rights Reserved
*
* This code is released under the GNU Affero General Public License.
*
* See: http://www.fsf.org/licensing/licenses/agpl-3.0.html
*
* VLADIMIR RODIONOV MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
* OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
* IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
* NON-INFRINGEMENT. Vladimir Rodionov SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED
* BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR
* ITS DERIVATIVES.
*
* Author: Vladimir Rodionov
*
*******************************************************************************/
package com.inclouds.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;


// TODO: Auto-generated Javadoc
/**
 * The Class HBaseAppendTest.
 */
public class HBaseAppendTest extends TestCase{

	
	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(CoprocessorBaseTest.class);	  
	
	/** The util. */
	private static HBaseTestingUtility UTIL = new HBaseTestingUtility();	
	
	/** The table a. */
	protected byte[] TABLE_A = "TABLE_A".getBytes();
	
	/** The families. */
	protected byte[][] FAMILIES = new byte[][]
	    {"fam_a".getBytes(), "fam_b".getBytes(), "fam_c".getBytes()};
	
	/** The columns. */
	protected  byte[][] COLUMNS = 
		{"col_a".getBytes(), "col_b".getBytes(),  "col_c".getBytes()};
	
	/** The versions. */
	int VERSIONS = 10;	
	
	/** The cluster. */
	MiniHBaseCluster cluster;
	
	/** The table desc. */
	HTableDescriptor tableDesc; 	
	
	/** The table. */
	HTable table;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	public void setUp() throws Exception {
//		ConsoleAppender console = new ConsoleAppender(); // create appender
//		// configure the appender
//		String PATTERN = "%d [%p|%c|%C{1}] %m%n";
//		console.setLayout(new PatternLayout(PATTERN));
//		console.setThreshold(Level.ERROR);
//
//		console.activateOptions();
//		// add appender to any Logger (here is root)
//		Logger.getRootLogger().removeAllAppenders();
//		Logger.getRootLogger().addAppender(console);
		Configuration conf = UTIL.getConfiguration();
		conf.set("hbase.zookeeper.useMulti", "false");
		UTIL.startMiniCluster(1);
		cluster = UTIL.getMiniHBaseCluster();
	    createHBaseTable();	    
	    
	}	
	
	/**
	 * Creates the HBase table.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void createHBaseTable() throws IOException {		
		
		LOG.error("Create HBase table and put data");
		HColumnDescriptor famA = new HColumnDescriptor(FAMILIES[0]);		
		famA.setMaxVersions(VERSIONS);
		
		HColumnDescriptor famB = new HColumnDescriptor(FAMILIES[1]);
		famB.setMaxVersions(VERSIONS);		

		HColumnDescriptor famC = new HColumnDescriptor(FAMILIES[2]);
		famC.setMaxVersions(VERSIONS);	
		
		tableDesc = new HTableDescriptor(TABLE_A);
		tableDesc.addFamily(famA);
		tableDesc.addFamily(famB);
		tableDesc.addFamily(famC);
		
		Configuration cfg = cluster.getConf();
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if( admin.tableExists(tableDesc.getName()) == false){
			admin.createTable(tableDesc);
			LOG.error("Created table "+tableDesc);
		}

		table = new HTable(cfg, TABLE_A);	
		// Create row
		List<KeyValue> rowData = generateRowData();
		Put put = createPut(rowData);
		// Put data
		table.put(put);
		LOG.error("Finished.");		
		
	}	
	
	/**
	 * Creates the put.
	 *
	 * @param values the values
	 * @return the put
	 */
	protected Put createPut(List<KeyValue> values)
	{
		Put put = new Put(values.get(0).getRow());
		for(KeyValue kv: values)
		{
			put.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
		}
		return put;
	}
		
	/**
	 * Generate row data.
	 *
	 * @return the list
	 */
	List<KeyValue> generateRowData(){
		byte[] row = "row".getBytes();
		byte[] value = "value".getBytes();		
		long startTime = System.currentTimeMillis();
		ArrayList<KeyValue> list = new ArrayList<KeyValue>();
		int count = 0;
		for(byte[] f: FAMILIES){
			for(byte[] c: COLUMNS){
				count = 0;
				for(; count < VERSIONS; count++){
					KeyValue kv = new KeyValue(row, f, c, startTime - 1000*(count),  value);	
					list.add(kv);
				}
			}
		}		
		Collections.sort(list, KeyValue.COMPARATOR);		
		return list;
	}
	
	/**
	 * Creates the append.
	 *
	 * @param row the row
	 * @param families the families
	 * @param columns the columns
	 * @param value the value
	 * @return the append
	 */
	protected Append createAppend(byte[] row, List<byte[]> families, List<byte[]> columns, byte[] value){
		
		Append op = new Append(row);
		
		for(byte[] f: families){
			for(byte[] c: columns){
				op.add(f, c, value);
			}
		}
		return op;
	}
	
	/**
	 * Test append.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testAppend() throws IOException
	{
		LOG.error("Test append started. Testing row: 'row'");
		
		byte[] row ="row".getBytes();
		byte[] toAppend = "_appended".getBytes();
		
		Get get = new Get(row);
		get.setMaxVersions(Integer.MAX_VALUE);
		Result result = table.get(get);	
		assertEquals(90, result.size() );
		
		Append append = createAppend(row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS), toAppend);
		Result r = table.append(append);		
		assertEquals(9, r.size());

		get = new Get(row);
		get.setMaxVersions(Integer.MAX_VALUE);
		result = table.get(get);
		assertEquals (90, result.size());
		LOG.error("Test append finished.");
	}
}
