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
import java.util.NavigableSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

// TODO: Auto-generated Javadoc
/**
 * The Class CoprocessorBasicOpsTest.
 */
public class CoprocessorBasicOpsTest extends CoprocessorBaseTest{

	/** The n. */
	int N = 1000;
	
	/**
	 * Test put all.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testAll() throws IOException
	{
		putAllData(_tableA, N);
		_testFirstGet();
		_testSecondGet();
		_testThirdGet();
		_testThirdGetBatch();
		_testThirdGetBatchCacheDisabled();
		_testExistsInCache();
	}
	
	/**
	 * Test load co-processor.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void _testFirstGet() throws IOException
	{
		
		LOG.error("Test first get started");
		
		long start = System.currentTimeMillis();
		for(int i=0 ; i< N; i++){		
			Get get = createGet(data.get(i).get(0).getRow(), null, null, null);
			get.setMaxVersions(Integer.MAX_VALUE);
			Result result = _tableA.get(get);		
			List<KeyValue> list = result.list();
			assertEquals(data.get(i).size(), list.size());	
			assertEquals(0, cache.getFromCache());
			
		}
		LOG.error("Test first get finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * Test second get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void _testSecondGet() throws IOException
	{
		
		LOG.error("Test second get started");
		
		long start = System.currentTimeMillis();
		for(int i=0 ; i< N; i++){		
			Get get = createGet(data.get(i).get(0).getRow(), null, null, null);
			get.setMaxVersions(Integer.MAX_VALUE);
			Result result = _tableA.get(get);		
			List<KeyValue> list = result.list();
			assertEquals(data.get(i).size(), list.size());	
			assertEquals(list.size(), cache.getFromCache());
			
		}
		LOG.error("Test second get finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * _test third get.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void _testThirdGet() throws IOException
	{
		
		LOG.error("Test third (1 narrow) get started");
		
		long start = System.currentTimeMillis();
		List<byte[]> fam = new ArrayList<byte[]>();
		fam.add(FAMILIES[0]);
		List<byte[]> col = new ArrayList<byte[]>();
		col.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);
		
		for(int i=0 ; i< N; i++){		
			Get get = createGet(data.get(i).get(0).getRow(), map, null, null);
			get.setMaxVersions(1);
			Result result = _tableA.get(get);			
			List<KeyValue> list = result.list();
			assertEquals(1, list.size());	
			assertEquals(list.size(), cache.getFromCache());
			
		}
		LOG.error("Test third get finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * _test third get batch.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void _testThirdGetBatch() throws IOException
	{
		
		LOG.error("Test third (1 narrow) get batch started");
		int BATCH_SIZE = 100;
		long start = System.currentTimeMillis();
		List<byte[]> fam = new ArrayList<byte[]>();
		fam.add(FAMILIES[0]);
		List<byte[]> col = new ArrayList<byte[]>();
		col.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);
		
		for(int i=0 ; i< N; i += BATCH_SIZE){		
			List<Get> batch = new ArrayList<Get>();
			for(int k =0; k < BATCH_SIZE; k++){
				Get get = createGet(data.get(i).get(0).getRow(), map, null, null);
				get.setMaxVersions(1);
				batch.add(get);
			}
									
			Result[] result = _tableA.get(batch);	
			assertEquals(BATCH_SIZE, result.length);
//			for( int j =0; j < result.length; j++){				
//				assertEquals(1, result[j].size());
//			}
			
		}
		LOG.error("Test third get batch finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * _test third get batch cache disabled.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void _testThirdGetBatchCacheDisabled() throws IOException
	{
		
		LOG.error("Test third (1 narrow) get batch cache disabled started");
		cache.setDisabled(true);
		int BATCH_SIZE = 100;
		long start = System.currentTimeMillis();
		List<byte[]> fam = new ArrayList<byte[]>();
		fam.add(FAMILIES[0]);
		List<byte[]> col = new ArrayList<byte[]>();
		col.add(COLUMNS[0]);
		
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(fam, col);
		
		for(int i=0 ; i< N; i += BATCH_SIZE){		
			List<Get> batch = new ArrayList<Get>();
			for(int k =0; k < BATCH_SIZE; k++){
				Get get = createGet(data.get(i).get(0).getRow(), map, null, null);
				get.setMaxVersions(1);
				batch.add(get);
			}
									
			Result[] result = _tableA.get(batch);	
			assertEquals(BATCH_SIZE, result.length);
			
//			
//			List<KeyValue> list = result.list();
//			assertEquals(1, list.size());	
//			assertEquals(list.size(), cache.getFromCache());
			
		}
		LOG.error("Test third get batch cache disabled finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	
	/**
	 * _test exists in cache.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void _testExistsInCache() throws IOException
	{
		
		LOG.error("Test exists in Cache started");
		// Enable row-cache
		cache.setDisabled(false);
		long start = System.currentTimeMillis();
		for(int i=0 ; i< N; i++){		
			Get get = createGet(data.get(i).get(0).getRow(), null, null, null);
			get.setMaxVersions(1);
			boolean result = _tableA.exists(get);	
			assertTrue(result);
			
			assertEquals(FAMILIES.length * COLUMNS.length, cache.getFromCache());
			
		}
		LOG.error("Test exists in Cache finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
}
