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
package com.koda.test;

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.config.CacheConfiguration;

public class CacheUpdateTest extends TestCase{
	private final static Logger LOG = Logger
	.getLogger(CacheUpdateTest.class);
	
	static OffHeapCache sCache;
	static int N = 1000000;
	/** The SIZE. */
	private static int SIZE = 100;
	static long memoryLimit = 250000000;
	static{
		try {
			initCache();
		} catch (KodaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void initCache() throws KodaException {
		CacheManager manager = CacheManager.getInstance();
		CacheConfiguration config = new CacheConfiguration();
		config.setBucketNumber(N);
		config.setMaxMemory(memoryLimit);
		config.setEvictionPolicy("LRU");
		//config.setDefaultExpireTimeout(10);
		config.setCacheName("test");
		sCache = manager.createCache(config);
		
	}
	
	
	public void testBulkPut() throws NativeMemoryException {
		
		LOG.info("Test Bulk Put/Update:" + Thread.currentThread().getName());
		String key = "key";
		long t1 = System.currentTimeMillis();
		int M = 2;
		byte[] buffer = new byte[SIZE];
		org.yamm.util.Utils.memset(buffer, (byte)5);
		try {

			for(int k = 0; k < M ; k++){
				for (int i = 0; i < N; i++) {
					String s = key + i;

					if( k == 1){
						//LOG.info(""+i);
						if(sCache.get(s) == null){
							//byte[] buf = (byte[])sCache.get(s);
							LOG.info("not found: "+ s+" eviction is "+sCache.isEvictionActive()+" buf is null? ");
						}
					}
					//if( k == 0 ){
					
					//if( k == 1 && i == 0)
					//LOG.info("before "+k+": put "+s+" : key24 =" + (sCache.get("key24") != null));
					sCache.put(s, buffer);
					//LOG.info(k+": put "+s+" : key24 =" + (sCache.get("key24") != null));
					//}
				}
				LOG.info("Cache size="+sCache.getAllocatedMemorySize()+" : items="+sCache.size());
			}
			

			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		long t2 = System.currentTimeMillis();
		LOG.info(Thread.currentThread().getName() + "-" + (N*M) + " puts in "
				+ (t2 - t1) + " ms" + "; cache size =" + sCache.size()
				+ " Memory =" + sCache.getAllocatedMemorySize());
	}
	

}
