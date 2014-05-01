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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;

import com.inclouds.hbase.rowcache.RowCache;
import com.inclouds.hbase.rowcache.RowCacheCoprocessor;


// TODO: Auto-generated Javadoc
/**
 * The Class CoprocessorLoadTest.
 */
public class CoprocessorBaseTest extends BaseTest{

	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(CoprocessorBaseTest.class);
	  
	/** The util. */
	private static HBaseTestingUtility UTIL = new HBaseTestingUtility();	
	
	/** The cp class name. */
	private static String CP_CLASS_NAME = RowCacheCoprocessor.class.getName();
	
	/** The n. */
	int N = 10000;
	
	
	/** The cluster. */
	MiniHBaseCluster cluster;
	
	/** The cache. */
	RowCache cache;
	
	/** The _table c. */
	HTable _tableA, _tableB, _tableC;
	
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
		conf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, CP_CLASS_NAME);
		conf.set("hbase.zookeeper.useMulti", "false");

    // Cache configuration
    conf.set(RowCache.ROWCACHE_MAXMEMORY, "1000000000");
    //conf.set(CacheConfiguration.EVICTION_POLICY, "LRU");
    conf.set(RowCache.ROWCACHE_MAXITEMS, "10000000");
    conf.set(RowCache.ROWCACHE_COMPRESSION, "LZ4");

		// Enable snapshot
		UTIL.startMiniCluster(1);
		
		// Row Cache
		if( data != null) return;		
		data = generateData(N);	 
		cluster = UTIL.getMiniHBaseCluster();
	    createTables(VERSIONS);
	    createHBaseTables();
	    
		while( cache == null){
			cache = RowCache.instance;
			Thread.sleep(1000);
			LOG.error("WAIT 1s for row cache to come up");
		}
		LOG.error("cache = "+cache);
	    
	}
	
	

	/* (non-Javadoc)
	 * @see com.inclouds.hbase.test.BaseTest#createTables()
	 */

	/**
	 * Creates the h base tables.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void createHBaseTables() throws IOException {		
		Configuration cfg = cluster.getConf();
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if( admin.tableExists(tableA.getName()) == false){
			admin.createTable(tableA);
			LOG.error("Created table "+tableA);
		}
		if( admin.tableExists(tableB.getName()) == false){
			admin.createTable(tableB);
			LOG.error("Created table "+tableB);
		}	
		if( admin.tableExists(tableC.getName()) == false){
			admin.createTable(tableC);
			LOG.error("Created table "+tableC);
		}	
		_tableA = new HTable(cfg, TABLE_A);		
		_tableB = new HTable(cfg, TABLE_B);
		_tableC = new HTable(cfg, TABLE_C);
		
	}



	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	@Override 
	public void tearDown() throws Exception {
	    LOG.error("\n Tear Down the cluster and test \n");
		//Thread.sleep(2000);
	    //UTIL.shutdownMiniCluster();
	}
	  


	/**
	 * Put all data.
	 *
	 * @param table the table
	 * @param n the n
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void putAllData(HTable table, int n) throws IOException
	{
		LOG.error ("Put all " + n +" rows  starts.");
		long start = System.currentTimeMillis();
		for(int i=0; i < n; i++){
			Put put = createPut(data.get(i));
			table.put(put);
		}
		table.flushCommits();
		LOG.error ("Put all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
	}
	
	/**
	 * Delete all data.
	 *
	 * @param table the table
	 * @param n the n
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected void deleteAllData(HTable table, int n) throws IOException
	{
		LOG.error ("Delete all " + n +" rows  starts.");
		long start = System.currentTimeMillis();
		for(int i=0; i < n; i++){
			Delete delete = createDelete(data.get(i).get(0).getRow());
			table.delete(delete);
		}
		table.flushCommits();
		LOG.error ("Delete all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
	}

	/**
	 * Filter.
	 *
	 * @param list the list
	 * @param fam the fam
	 * @param col the col
	 * @return the list
	 */
	protected List<KeyValue> filter (List<KeyValue> list, byte[] fam, byte[] col)
	{
		List<KeyValue> newList = new ArrayList<KeyValue>();
		for(KeyValue kv: list){
			if(doFilter(kv, fam, col)){
				continue;
			}
			newList.add(kv);
		}
		return newList;
	}
	
	/**
	 * Do filter.
	 *
	 * @param kv the kv
	 * @param fam the fam
	 * @param col the col
	 * @return true, if successful
	 */
	private final boolean doFilter(KeyValue kv, byte[] fam, byte[] col){
		if (fam == null) return false;
		byte[] f = kv.getFamily();
		if(Bytes.equals(f, fam) == false) return true;
		if( col == null) return false;
		byte[] c = kv.getQualifier();
		if(Bytes.equals(c, col) == false) return true;
		return false;
	}
	
	/**
	 * Dump put.
	 *
	 * @param put the put
	 */
	protected void dumpPut(Put put) {
		Map<byte[], List<KeyValue>> map = put.getFamilyMap();
		for(byte[] row: map.keySet()){
			List<KeyValue> list = map.get(row);
			for(KeyValue kv : list){
				LOG.error(kv);
			}
		}
		
	}
}
