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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;

// TODO: Auto-generated Javadoc
/**
 * The Class CoprocessorGetTest.
 * Tests Get/Exists
 * 
 */
public class CoprocessorDMLTest extends CoprocessorBaseTest{

	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(CoprocessorDMLTest.class);
	/** The n. */
	int N = 1000;
	
	HBaseAdmin admin;
	
	/**
	 * Test put all.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testAll() throws IOException
	{
		//cache.setDisabled(true);
		putAllData(_tableA, N);
		_testGetFromHBase();
		_testDisableEnableTable();

	}
	
	@Override
	public void setUp() throws Exception
	{
		if(admin != null) return;
		
		super.setUp();		
		Configuration cfg = cluster.getConf();
		admin = new HBaseAdmin(cfg);
		
	}
	public void _testGetFromHBase() throws IOException
	{
		
		LOG.error("Test get from HBase started");
		
		long start = System.currentTimeMillis();
		for(int i=0 ; i< N; i++){		
			Get get = createGet(data.get(i).get(0).getRow(), null, null, null);
			get.setMaxVersions(Integer.MAX_VALUE);
			Result result = _tableA.get(get);		
			List<KeyValue> list = result.list();
			assertEquals(data.get(i).size(), list.size());			
			
		}
		assertEquals(N * FAMILIES.length, cache.size());	
		LOG.error("Test get from HBase finished in "+(System.currentTimeMillis() - start)+"ms");
		
	}
	private void _testDisableEnableTable() throws IOException
	{
		LOG.error("Test Disable-Enable Table started.");
		assertFalse(cache.isDisabled());
		long cacheSize = cache.size();
		LOG.error("Cache size before disabling ="+cacheSize);
		cache.setTrace(true);
		admin.disableTable(TABLE_A);
		assertEquals(cache.size(), cacheSize);
		// TODO verify that
		//assertTrue(cache.isDisabled());
		admin.enableTable(TABLE_A);
		assertFalse(cache.isDisabled());
		assertEquals(cache.size(), cacheSize);
		LOG.error("Test Disable-Enable Table finished.");
	}
	
	@SuppressWarnings("unused")
  private void waitForCacheEnabled()
	{
		while(cache.isDisabled()){
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	

}
