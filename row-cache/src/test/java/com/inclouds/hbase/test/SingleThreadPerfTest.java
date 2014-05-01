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
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;

import com.inclouds.hbase.rowcache.RowCache;


// TODO: Auto-generated Javadoc
/**
 * The Class SingleThreadPerfTest.
 */
public class SingleThreadPerfTest extends BaseTest{
	  /** The Constant LOG. */
  	static final Log LOG = LogFactory.getLog(SingleThreadPerfTest.class);
	// Total number of Rows to test
	/** The n. */
	int N = 100000;
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
  protected void setUp() throws Exception {
    if( data != null) return;
    
    
    data = generateData(N);
    
    Configuration conf = new Configuration();
      // Cache configuration
      conf.set(RowCache.ROWCACHE_MAXMEMORY, "1000000000");
      //conf.set(CacheConfiguration.EVICTION_POLICY, "LRU");
      conf.set(RowCache.ROWCACHE_MAXITEMS, "10000000");
      conf.set(RowCache.ROWCACHE_COMPRESSION, "LZ4");
      
      cache = new RowCache();
      cache.start(conf);
      
      createTables(Integer.MAX_VALUE);
      
  }
	
	/**
	 * Test load time.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testLoadTime () throws IOException
	{
		LOG.info("Test load started");
		int M = 1;
		long start = System.currentTimeMillis();
		for(int k = 0; k < M; k++){
			for(int i =0; i < N; i++){
				cacheRow(tableA, i);
			}
			LOG.info("Cache size  ="+ cache.getOffHeapCache().getAllocatedMemorySize()+": items ="+cache.getOffHeapCache().size());
		}
		LOG.info("Loading "+ (N * M * FAMILIES.length) +" full rows"+(COLUMNS.length * VERSIONS)+ " KV's each) took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getOffHeapCache().getAllocatedMemorySize());
		LOG.info("Cache items ="+cache.getOffHeapCache().size());
		LOG.info("Test load finished");
		
	}
	
	/**
	 * Test get full row.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testGetFullRow() throws IOException
	{
		LOG.info("Test get full row started");
		int M = 10;
		long start = System.currentTimeMillis();
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), null);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i =0; i < N ; i++){
				List<KeyValue> list = data.get(i);
				Get get = createGet(data.get(i).get(0).getRow(), map, null, null);
				get.setMaxVersions(VERSIONS);
				List<KeyValue> results = new ArrayList<KeyValue>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size(), results.size());
				assertTrue(equals(list, results));
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:family (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" full rows (30 KV's each) took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getOffHeapCache().getAllocatedMemorySize());
		LOG.info("Cache items ="+cache.getOffHeapCache().size());
		LOG.info("Test get full finished");
	}
	
	/**
	 * Test get full row perf.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testGetFullRowPerf() throws IOException
	{
		LOG.info("Test get full row started - PERF");
		int M = 10;
		long start = System.currentTimeMillis();
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), null);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i =0; i < N ; i++){
				List<KeyValue> list = data.get(i);
				Get get = createGet(data.get(i).get(0).getRow(), map, null, null);
				get.setMaxVersions(VERSIONS);
				List<KeyValue> results = new ArrayList<KeyValue>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size(), results.size());
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:family (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" full rows (30 KV's each) took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getOffHeapCache().getAllocatedMemorySize());
		LOG.info("Cache items ="+cache.getOffHeapCache().size());
		LOG.info("Test get full finished - PERF");
	}
	

	
	/**
	 * Test get full row perf last version.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testGetFullRowPerfLastVersion() throws IOException
	{
		LOG.info("Test get full row started - PERF LAST VERSION ONLY");
		int M = 10;
		long start = System.currentTimeMillis();
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), null);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i =0; i < N ; i++){
				List<KeyValue> list = data.get(i);
				Get get = createGet(data.get(i).get(0).getRow(), map, null, null);
				get.setMaxVersions(1);
				List<KeyValue> results = new ArrayList<KeyValue>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size()/VERSIONS, results.size());
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:family (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" full rows "+ (COLUMNS.length)+ " each took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getOffHeapCache().getAllocatedMemorySize());
		LOG.info("Cache items ="+cache.getOffHeapCache().size());
		LOG.info("Test get full finished - PERF LAST VERSION");
	}
	
	/**
	 * Test get row fam col perf last version.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testGetRowFamColPerfLastVersion() throws IOException
	{
		LOG.info("Test get row:fam:col started - PERF LAST VERSION ONLY");
		int M = 10;
		long start = System.currentTimeMillis();
		List<byte[]> cols = new ArrayList<byte[]>();
		cols.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(Arrays.asList(FAMILIES), cols);
		for(int  k = 0; k < M; k++){
			long t = System.currentTimeMillis();
			for(int i =0; i < N ; i++){
				List<KeyValue> list = data.get(i);
				Get get = createGet(data.get(i).get(0).getRow(), map, null, null);
				get.setMaxVersions(1);
				List<KeyValue> results = new ArrayList<KeyValue>();
				boolean bypass = cache.preGet(tableA, get, results);
				assertTrue(bypass);
				assertEquals(list.size()/(VERSIONS * COLUMNS.length), results.size());
				
			}
			LOG.info("Get "+ (FAMILIES.length * N)  +" row:fam:col (s) in "+ (System.currentTimeMillis() - t)+" ms");
		}
		LOG.info("Reading "+ (N * M * FAMILIES.length) +" row:fam:col "+ (COLUMNS.length)+ " each took "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Cache size  ="+ cache.getOffHeapCache().getAllocatedMemorySize());
		LOG.info("Cache items ="+cache.getOffHeapCache().size());
		LOG.info("Test get row:fam:col finished - PERF LAST VERSION");
	}	
}
