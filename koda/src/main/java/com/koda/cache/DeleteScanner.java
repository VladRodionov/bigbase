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
package com.koda.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.koda.util.Configuration;


// TODO: Auto-generated Javadoc
/**
 * The Class Query.
 *
 * @param <T> the generic type
 */
public class DeleteScanner {
	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(DeleteScanner.class);	
	
	/** The m filter. */
	private Filter mFilter;
	
	/** The m scan class. */
	private Class<?> mScanClass;
	
	
	/**
	 * Instantiates a new query.
	 *
	 * @param filter the filter
	 * @param scanClass the scan class
	 * @param reduce the reduce
	 */
	public DeleteScanner(Filter filter )
	{
		this.mFilter = filter;
		
		if(mScanClass == null ) 
			throw new IllegalArgumentException("null scan or reduce");
	}
	
	/**
	 * Execute query on cache.
	 *
	 * @param cache the cache
	 * @param cfg the cfg
	 * @return result
	 * @throws ExecutionException the execution exception
	 * @throws InterruptedException the interrupted exception
	 */
	
	/** TODO - config object*/

	public long execute(OffHeapCache cache, Configuration cfg) throws ExecutionException, InterruptedException 
	{ 
		ExecutorService service = OffHeapCache.getQueryExecutorService();
		int maxWorkers = OffHeapCache.getQueryMaxProcessors();
		LOG.info("Max processors="+maxWorkers);
		
		List<ScanRunner<Long>> scanRunners = new ArrayList<ScanRunner<Long>>(maxWorkers);
		
		for(int i=0; i < maxWorkers; i++)
		{
				
			DeleteScan scan = new DeleteScan(cache);				
			scan.configure(cfg);
			ScanRunner<Long> sr = new  ScanRunner<Long>( new CacheScanner(cache, i, maxWorkers, false), mFilter, scan);
			scanRunners.add(sr);
		}
			
		List<Future<Long>> futures = service.invokeAll(scanRunners);

		long result = 0;
		for(Future<Long> f: futures){
			result += f.get();
		}
			
		
		return result;
	}
	
	
	class DeleteScan implements Scan<Long>{

		OffHeapCache cache;
		long totalDeleted;
		
		public DeleteScan(OffHeapCache cache)
		{
			this.cache = cache;
		}
		
		
		@Override
		public void configure(Configuration cfg) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void finish() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Long getResult() {
			// TODO Auto-generated method stub
			return totalDeleted;
		}

		@Override
		public void scan(long ptr) {
			
			totalDeleted++;
			
		}
		
	}
}
