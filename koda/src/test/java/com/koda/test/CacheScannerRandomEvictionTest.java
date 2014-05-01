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

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;

import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.CacheScanner;
import com.koda.cache.OffHeapCache;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;


import junit.framework.TestCase;
@SuppressWarnings("unused")
public class CacheScannerRandomEvictionTest extends TestCase
{
	private final static Logger LOG = Logger.getLogger(CacheScannerRandomEvictionTest.class);
	static OffHeapCache cache;
  
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
	  if(cache != null) return;
		CacheManager manager = CacheManager.getInstance();	
		CacheConfiguration cfg = new CacheConfiguration();
		// Max memory = 100MB
		cfg.setMaxMemory(100000000);
		// Bucket number 
		cfg.setBucketNumber(1000000);
		cfg.setCodecType(CodecType.LZ4);
		cfg.setEvictionPolicy("LRU");
		cache = manager.createCache(cfg);
		fillUpCache();
	}
	
	public void fillUpCache() throws NativeMemoryException, IOException
	{
		LOG.info("+++Inserting "+cache.getTotalBuckets()+" objects");
		
		long start= System.currentTimeMillis();				
		String key = "key-";
		String value = "value-";
		int num = cache.getTotalBuckets();
		for(int i=0; i < num; i++){
			try{
			  if( i == 1000000){
			    System.out.println();
			  }
		  cache.put(key+i, value+i);
			}catch(Throwable e){
			  LOG.error("FAILED "+i+" eviction active="+cache.isEvictionActive()+" mem="+cache.getTotalAllocatedMemorySize());
			}
		}
		LOG.info("+++ Done "+cache.getTotalBuckets()+" in "+(System.currentTimeMillis() - start));
		
		
	}
	
	public void testFullCacheScanner() throws NativeMemoryException, IOException
	{
		LOG.info("Test full cache scanner starts");
		long start = System.currentTimeMillis();
		int totalScanned = 0;
		long totalInCache = cache.size();
		int startIndex = 0;
		int endIndex = cache.getTotalBuckets() - 1;
		CacheScanner scanner = new CacheScanner( cache, startIndex, endIndex, 0 );
		while(scanner.hasNext()){
			Object key = scanner.nextKey();
			assertNotNull(key);
			totalScanned++;
		}
		scanner.close();
		LOG.info("Scanned "+totalScanned+" objects in "+(System.currentTimeMillis() - start)+"ms");
		assertEquals(totalInCache, totalScanned);
		LOG.info("Test full cache scanner finished.");
		
	}
	
	public void testPerf() throws NativeMemoryException, IOException
	{
		LOG.info("Test performance starts");
		long start = System.currentTimeMillis();
		int samples = 1000;
		int iterations = 10000;
		Random r = new Random();
		int startIndex = 0;
		int endIndex = cache.getTotalBuckets() - 1;
		long totalScanned = 0;
		for(int i=0; i < iterations; i++){
			startIndex = r.nextInt(endIndex/2);
			CacheScanner scanner = new CacheScanner( cache, startIndex, endIndex, 0 );
			int count= 0;
			while(scanner.hasNext() && count++ < samples){
				Object key = scanner.nextKey();
				assertNotNull(key);
				totalScanned++;
			}
			scanner.close();
		}
		
		LOG.info("Test performance ends. "+iterations+
				" of "+samples+" in "+(System.currentTimeMillis() - start)+"ms. Scanned="+totalScanned);
	}
	
	public void testRandomSample() throws NativeMemoryException, IOException
	{
		int N = 100;
		LOG.info("Test random sample starts. Get "+N+" random keys");
		Object[] keys = cache.getRandomKeys(N);
		assertEquals(N, keys.length);
		for(int i=0; i < N; i++){
			assertNotNull(keys[i]);
			LOG.info(keys[i]);
		}
		LOG.info("Test random keys finished.");
	}
	
	public void testGetEvictionData() throws NativeMemoryException, IOException
	{
		int N = 10;
		LOG.info("Test get eviction data starts. Get "+N+" random keys. Than get their eviction data");
		Object[] keys = cache.getRandomKeys(N);
		assertEquals(N, keys.length);
		for(int i=0; i < N; i++){
			assertNotNull(keys[i]);
			// touch eviction data
			Object value = cache.get(keys[i]);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			LOG.info(keys[i]+" eviction data="+ cache.getEvictionData(keys[i]));
		}
		LOG.info("Test get eviction data finished.");
	}
	
}
