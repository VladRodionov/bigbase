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
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

import com.inclouds.hbase.rowcache.RowCache;


// TODO: Auto-generated Javadoc
/**
 * The Class RowCacheTest.
 */
public class RowCacheTest extends BaseTest{

	  /** The Constant LOG. */
  	static final Log LOG = LogFactory.getLog(RowCacheTest.class);	
	/* Tables */

  	/** The n. */
	protected int N = 10000;
  	
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
	 * Test simple put get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testAllRow() throws IOException
	{
		LOG.info("Test simple Put-Get started");
		
		// Take the whole row
		List<KeyValue> list = data.get(0);				
		Get get = createGet(list.get(0).getRow(), null, null, null);
		get.setMaxVersions(10);
		List<KeyValue> results = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, results);
		assertEquals(0, results.size());
		cache.postGet(tableA, get, list);
		get = createGet(list.get(0).getRow(), null, null, null);
		get.setMaxVersions(10);
		cache.preGet(tableA, get, results);		
		assertEquals(list.size(), results.size());		
		assertTrue( equals(list, results));
		LOG.info("Test simple Put-Get finished");		
		
	}
	
	/**
	 * Test single family.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testSingleFamily() throws IOException
	{
		LOG.info("Test single family started");
		List<KeyValue> list = data.get(0);	
		
		List<KeyValue> toCmp = subList(list, list.get(0).getFamily());
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(list.get(0).getFamily(), null);
		
		Get get = createGet(list.get(0).getRow(), map, null, null);
		get.setMaxVersions(10);	
		List<KeyValue> results = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, results);
		assertEquals(COLUMNS.length * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family finished");
		
	}
	
	/**
	 * Test single family one column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testSingleFamilyOneColumn() throws IOException
	{
		LOG.info("Test single family one column started");
		List<KeyValue> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[1]});
		List<KeyValue> toCmp = subList(list, list.get(0).getFamily(), cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(list.get(0).getFamily(), getColumnSet(cols));
		
		Get get = createGet(list.get(0).getRow(), map, null, null);
		get.setMaxVersions(10);	
		List<KeyValue> results = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, results);
		assertEquals(VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family one column finished");
		
	}
	
	/**
	 * Test single family two columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testSingleFamilyTwoColumns() throws IOException
	{
		LOG.info("Test single family two columns started");
		List<KeyValue> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[0], COLUMNS[1]});
		List<KeyValue> toCmp = subList(list, list.get(0).getFamily(), cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(list.get(0).getFamily(), getColumnSet(cols));
		
		Get get = createGet(list.get(0).getRow(), map, null, null);
		get.setMaxVersions(10);	
		List<KeyValue> results = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, results);
		assertEquals(2 * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family two columns finished");
		
	}

	
	/**
	 * Test single family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testSingleFamilyThreeColumns() throws IOException
	{
		LOG.info("Test single family three columns started");
		List<KeyValue> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[0], COLUMNS[1], COLUMNS[2]});
		List<KeyValue> toCmp = subList(list, list.get(0).getFamily(), cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		map.put(list.get(0).getFamily(), getColumnSet(cols));
		
		Get get = createGet(list.get(0).getRow(), map, null, null);
		get.setMaxVersions(VERSIONS);	
		List<KeyValue> results = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, results);
		assertEquals(3 * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test single family three columns finished");
		
	}	
	
	/**
	 * Test two family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testTwoFamilyThreeColumns() throws IOException
	{
		LOG.info("Test two family three columns started");
		List<KeyValue> list = data.get(0);	
		List<byte[]> cols = Arrays.asList( new byte[][]{COLUMNS[0], COLUMNS[1], COLUMNS[2]});
		List<byte[]> fams = Arrays.asList(new byte[][]{FAMILIES[0], FAMILIES[1]});
		List<KeyValue> toCmp = subList(list, fams, cols);
		
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		
		map.put(FAMILIES[0], getColumnSet(cols));
		map.put(FAMILIES[1], getColumnSet(cols));
		
		Get get = createGet(list.get(0).getRow(), map, null, null);
		get.setMaxVersions(VERSIONS);	
		List<KeyValue> results = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, results);
		assertEquals(6 * VERSIONS, results.size());
		assertTrue( equals(toCmp, results));
		LOG.info("Test two family three columns finished");
		
	}	
	
	
	/**
	 * Test two family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testTwoFamilyThreeColumnsMaxVersions() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") started");
			List<KeyValue> list = data.get(0);
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });
			List<byte[]> fams = Arrays.asList(new byte[][] { FAMILIES[0],
					FAMILIES[1] });
			List<KeyValue> toCmp = subList(list, fams, cols, maxVersions);

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));

			Get get = createGet(list.get(0).getRow(), map, null, null);
			get.setMaxVersions(maxVersions);
			List<KeyValue> results = new ArrayList<KeyValue>();
			cache.preGet(tableA, get, results);
			assertEquals(6 * maxVersions, results.size());
			assertEquals(toCmp.size(), results.size());
			assertTrue(equals(toCmp, results));
			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") finished");
		}
		
	}	
	
	
	/**
	 * Test two family three columns.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testTwoFamilyThreeColumnsMaxVersionsTimeRangePast() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range past started");
			List<KeyValue> list = data.get(0);
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });


			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));
			
			TimeRange tr = new TimeRange(0, System.currentTimeMillis() - ((long)24) * 3600 * 1000);
			
			Get get = createGet(list.get(0).getRow(), map, tr, null);
			get.setMaxVersions(maxVersions);
			List<KeyValue> results = new ArrayList<KeyValue>();
			cache.preGet(tableA, get, results);
			assertEquals(0, results.size());

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range past finished");
		}
		
	}	
	
	/**
	 * Test two family three columns max versions time range future.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testTwoFamilyThreeColumnsMaxVersionsTimeRangeFuture() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range future started");
			List<KeyValue> list = data.get(0);
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));
			
			TimeRange tr = new TimeRange(System.currentTimeMillis() + ((long)24)*3600 * 1000, 
					System.currentTimeMillis() + ((long)48) * 3600 * 1000);
			
			Get get = createGet(list.get(0).getRow(), map, tr, null);
			get.setMaxVersions(maxVersions);
			List<KeyValue> results = new ArrayList<KeyValue>();
			cache.preGet(tableA, get, results);
			assertEquals(0, results.size());

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range future finished");
		}
		
	}	
	
	/**
	 * Test two family three columns max versions time range present.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testTwoFamilyThreeColumnsMaxVersionsTimeRangePresent() throws IOException
	{
		
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range present started");
			List<KeyValue> list = data.get(0);
			
			//dump(list);
			
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0],
					COLUMNS[1], COLUMNS[2] });
			List<byte[]> fams = Arrays.asList(new byte[][] { FAMILIES[0],
					FAMILIES[1] });
			List<KeyValue> toCmp = subList(list, fams, cols, maxVersions);

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[1], getColumnSet(cols));
			
			TimeRange tr = new TimeRange(System.currentTimeMillis() - ((long)24)*3600 * 1000, 
					System.currentTimeMillis() + ((long)24) * 3600 * 1000);
			
			Get get = createGet(list.get(0).getRow(), map, tr, null);
			get.setMaxVersions(maxVersions);
			List<KeyValue> results = new ArrayList<KeyValue>();
			cache.preGet(tableA, get, results);
			assertEquals(6 * maxVersions, results.size());
			assertEquals(toCmp.size(), results.size());
			assertTrue(equals(toCmp, results));
			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range present finished");
		}
		
	}	
	
	/**
	 * Test family three columns max versions time range partial.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testFamilyThreeColumnsMaxVersionsTimeRangePartial() throws IOException
	{
		int M = 3;
		List<KeyValue> list = data.get(0);
		dump(list);
		for (int maxVersions = VERSIONS; maxVersions > 0; maxVersions--) {

			LOG.info("Test family three columns (max versions:"
					+ maxVersions + ") time range partial started");

			long maxTS = list.get(0).getTimestamp();
			
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0]});
			//List<byte[]> fams = Arrays.asList(new byte[][] { FAMILIES[0]});
			//List<KeyValue> toCmp = subList(list, fams, cols, maxVersions);

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));

			
			TimeRange tr = new TimeRange( maxTS - M, maxTS +1);
			
			Get get = createGet(list.get(0).getRow(), map, tr, null);
			get.setMaxVersions(maxVersions);
			List<KeyValue> results = new ArrayList<KeyValue>();
			cache.preGet(tableA, get, results);
			assertEquals( Math.min(M+1, maxVersions), results.size());
			//assertEquals(toCmp.size(), results.size());
			//assertTrue(equals(toCmp, results));
			LOG.info("Test two family three columns (max versions:"
					+ maxVersions + ") time range partial finished");
		}
		
	}	
	
	/**
	 * Test verify get after pre get call row not cached.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testVerifyGetAfterPreGetCallRowNotCached() throws IOException
	{
		LOG.info("Test verify get after preGet row not cached started");	
		int maxVersions = 5;
			// This row is not cached yet
			List<KeyValue> list = data.get(1);
			
			List<byte[]> cols = Arrays.asList(new byte[][] { COLUMNS[0], COLUMNS[2]});

			Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(
					Bytes.BYTES_COMPARATOR);

			map.put(FAMILIES[0], getColumnSet(cols));
			map.put(FAMILIES[2], getColumnSet(cols));
			
			TimeRange tr = new TimeRange( System.currentTimeMillis() - 100000, System.currentTimeMillis() );
			
			Get get = createGet(list.get(0).getRow(), map, tr, null);
			get.setMaxVersions(maxVersions);
			List<KeyValue> results = new ArrayList<KeyValue>();
			cache.preGet(tableA, get, results);
			
			assertEquals(Integer.MAX_VALUE, get.getMaxVersions());
			TimeRange trr = get.getTimeRange();
			assertEquals(0, trr.getMin());
			assertEquals(Long.MAX_VALUE, trr.getMax());
			assertNull(get.getFilter());
			// Assert families have no columns
			assertEquals(2, get.numFamilies());
			Map<byte[] , NavigableSet<byte[]>> fmap = get.getFamilyMap();
			assertTrue( fmap.containsKey(FAMILIES[0]));
			assertNull( fmap.get(FAMILIES[0]));
			assertTrue( fmap.containsKey(FAMILIES[2]));
			assertNull( fmap.get(FAMILIES[2]));
			assertFalse( fmap.containsKey(FAMILIES[1]));
			LOG.info("Test verify get after preGet row not cached finished");										
		
	}	
	
	/**
	 * Test put single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testPutSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Put single family:column started ");
		cacheRow(tableA, 1);		
		byte[] row = data.get(1).get(0).getRow();
		// Verify we have cached data
		List<KeyValue> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<KeyValue> putList = new ArrayList<KeyValue>();
		LOG.info("Put single family:column");
		KeyValue kv = new KeyValue(row, FAMILIES[0], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		putList.add(kv);
		Put put = createPut(putList);		
		cache.prePut(tableA, put);
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);	
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[1], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		putList.add(kv);
		put = createPut(putList);		
		cache.prePut(tableA, put);
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);	
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[2], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		putList.add(kv);
		put = createPut(putList);		
		cache.prePut(tableA, put);
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Put single family:column finished ");
		
	}
	
	/**
	 * Test delete single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testDeleteSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Delete single family:column started ");
		cacheRow(tableA, 1);
		byte[] row = data.get(1).get(0).getRow();
		// Verify we have cached data
		List<KeyValue> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<KeyValue> deleteList = new ArrayList<KeyValue>();
		LOG.info("Delete single family:column");
		KeyValue kv = new KeyValue(row, FAMILIES[0], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		deleteList.add(kv);
		Delete delete = createDelete(deleteList);		
		cache.preDelete(tableA, delete);
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[1], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		deleteList.add(kv);
		delete = createDelete(deleteList);		
		cache.preDelete(tableA, delete);
		
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		kv = new KeyValue(row, FAMILIES[2], COLUMNS[0], System.currentTimeMillis(), getValue(1));
		deleteList.add(kv);
		delete = createDelete(deleteList);		
		cache.preDelete(tableA, delete);
		
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Delete single family:column finished ");
		
	}
	
	/**
	 * Test increment single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testIncrementSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Increment single family:column started ");
		cacheRow(tableA, 1);
		byte[] row = data.get(1).get(0).getRow();
		// Verify we have cached data
		List<KeyValue> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns  = new ArrayList<byte[]>();
		families.add(FAMILIES[0]);
		columns.add(COLUMNS[0]);
		LOG.info("Increment single family:column");
		
		Increment incr = createIncrement(row, constructFamilyMap(families, columns), null, 1L);
		cache.preIncrement(tableA, incr, new Result());
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[1]);		
		incr = createIncrement(row, constructFamilyMap(families, columns), null, 1L);
		cache.preIncrement(tableA, incr, new Result());		
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[2]);		
		incr = createIncrement(row, constructFamilyMap(families, columns), null, 1L);
		cache.preIncrement(tableA, incr, new Result());	
		
		// Verify that we cleared row: FAMILIES[2] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Increment single family:column finished ");
		
	}
	
	/**
	 * Test append single family column.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testAppendSingleFamilyColumn() throws IOException
	{
		cache.resetRequestContext();
		
		LOG.info("Test Append single family:column started ");
		cacheRow(tableA, 1);
		byte[] row = data.get(1).get(0).getRow();
		// Verify we have cached data
		List<KeyValue> result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		
		assertEquals(FAMILIES.length * COLUMNS.length * VERSIONS, result.size());
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns  = new ArrayList<byte[]>();
		families.add(FAMILIES[0]);
		columns.add(COLUMNS[0]);
		LOG.info("Append single family:column");
		
		
		Append append = createAppend(row, families, columns, getValue(1));
		cache.preAppend(tableA, append);
		
		// Verify that we cleared row: FAMILIES[0] ONLY;
		Get get = new Get(row);
		get.addFamily(FAMILIES[0]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-1) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[1]);		
		append = createAppend(row, families, columns, getValue(1));
		cache.preAppend(tableA, append);	
		// Verify that we cleared row: FAMILIES[1] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[1]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-2) * COLUMNS.length * VERSIONS, result.size());
		
		// Put another family
		families.add(FAMILIES[2]);		
		append = createAppend(row, families, columns, getValue(1));
		cache.preAppend(tableA, append);
		
		// Verify that we cleared row: FAMILIES[2] ONLY;
		get = new Get(row);
		get.addFamily(FAMILIES[2]);
		
		result = new ArrayList<KeyValue>();
		cache.preGet(tableA, get, result);		
		// Result must be empty
		assertEquals(0, result.size());
		
		result = getFromCache(tableA, row, Arrays.asList(FAMILIES), Arrays.asList(COLUMNS));
		assertEquals((FAMILIES.length-3) * COLUMNS.length * VERSIONS, result.size());		
		LOG.info("Test Append single family:column finished ");
		
	}
	
	
	/**
	 * Test point get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testPointGet() throws IOException
	{
		cache.resetRequestContext();
		LOG.info("Test point Get started. We test full get request: row:family:column:version ");
		cacheRow(tableA, 1);
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns = new ArrayList<byte[]>();
		
		families.add(FAMILIES[2]);
		columns.add(COLUMNS[0]);
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);
		byte[] row = data.get(1).get(0).getRow();
		int expectedIndex = COLUMNS.length * VERSIONS * 2;
		
		long time =  data.get(1).get(expectedIndex).getTimestamp();
		TimeRange tr = new TimeRange(time, time +1);
		Get get = createGet(row, map, tr, null);

		List<KeyValue> results = new ArrayList<KeyValue>();
		List<KeyValue> expected = new ArrayList<KeyValue>();
		expected.add(data.get(1).get(expectedIndex));
		cache.preGet(tableA, get, results);
		
		assertEquals(1, results.size());		
		assertTrue(equals(expected, results));
		
		LOG.info("Test point Get finished. ");
		
	}
	
	/**
	 * Test multi point get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testMultiPointGet() throws IOException
	{
		cache.resetRequestContext();
		LOG.info("Test point Get started. We test full get request (multi): row:family:column:version ");
		cacheRow(tableA, 1);
		
		List<byte[]> families = new ArrayList<byte[]>();
		List<byte[]> columns = new ArrayList<byte[]>();
		
		families.add(FAMILIES[0]);
		families.add(FAMILIES[1]);
		families.add(FAMILIES[2]);
		columns.add(COLUMNS[0]);
		columns.add(COLUMNS[1]);
		columns.add(COLUMNS[2]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);
		byte[] row = data.get(1).get(0).getRow();
		
		Get get = createGet(row, map, null, null);
		get.setMaxVersions(1);
		
		List<KeyValue> results = new ArrayList<KeyValue>();
		List<KeyValue> expected = new ArrayList<KeyValue>();
		List<KeyValue> list = data.get(1);
		for(int i =0; i < list.size(); i += VERSIONS){
			expected.add(list.get(i));
		}

		cache.preGet(tableA, get, results);
		
		assertEquals(expected.size(), results.size());		
		assertTrue(equals(expected, results));
		
		LOG.info("Test multi-point Get finished. ");
		
	}
}
