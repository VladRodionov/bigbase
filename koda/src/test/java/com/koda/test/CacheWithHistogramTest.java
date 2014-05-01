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

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.common.util.NumericHistogram;
import com.koda.config.CacheConfiguration;

import junit.framework.TestCase;
@SuppressWarnings("unused")
public class CacheWithHistogramTest extends TestCase{
	
	private final static Logger LOG = Logger
	.getLogger(CacheWithHistogramTest.class);
	
	static OffHeapCache sCache;
	static int N = 1000000;
	/** The SIZE. */
	private static int SIZE = 100;
	static long memoryLimit = 100000000;
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
		// Enable histogram
		config.setHistogramEnabled(true);
		//config.setHistogramBins(1000);
		//config.setHistogramSamples(10000);
		config.setHistogramUpdateInterval(1000);

		config.setCacheName("test");
		sCache = manager.createCache(config);
		
	}
	
	public void testLoadCache() throws NativeMemoryException, IOException
	{
		LOG.info("Loading cache until its full ");
		String key  ="key-";
		String value = "value-";
		int total = 0;
		long startTime = System.currentTimeMillis();
		while(sCache.getTotalAllocatedMemorySize() < 0.9 * memoryLimit){
			sCache.put(key + total, value+total);
			total++;
			if(total % 100000 == 0 ){
			  LOG.info(total+ " allocatedMem="+ sCache.getTotalAllocatedMemorySize() +" mem limit="+memoryLimit+" size="+sCache.size());
			}
		}
		long endTime = System.currentTimeMillis();
		LOG.info("Done in "+(endTime - startTime)+"ms. Total objects="+total);
		long histogramUpdateInterval = sCache.getCacheConfiguration().getHistogramUpdateInterval();
		try {
			Thread.sleep(2* histogramUpdateInterval);
		} catch (InterruptedException e) {
			
		}
		NumericHistogram hist = sCache.getObjectHistogram();
		LOG.info("Checking histogram:");
		LOG.info("\n"+hist.toString(10));
		LOG.info("Duration from histogram="+
				(long)(hist.quantile(1) - hist.quantile(0))+"ms. Actual duration="+(endTime - startTime)+"ms");
		LOG.info("Done");
	}
}
