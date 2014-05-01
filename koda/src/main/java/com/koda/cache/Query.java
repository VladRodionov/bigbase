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
public class Query<T> {
	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(Query.class);	
	
	/** The m filter. */
	private Filter mFilter;
	
	/** The m scan class. */
	private Class<?> mScanClass;
	
	/** The m reduce. */
	private Reduce<T> mReduce;
	
	/**
	 * Instantiates a new query.
	 *
	 * @param filter the filter
	 * @param scanClass the scan class
	 * @param reduce the reduce
	 */
	public Query(Filter filter, Class<?> scanClass, Reduce<T> reduce )
	{
		this.mFilter = filter;
		
		this.mScanClass = scanClass;
		this.mReduce = reduce;		
		if(mScanClass == null || mReduce == null) 
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
	@SuppressWarnings("unchecked")
	public T execute(OffHeapCache cache, Configuration cfg) throws ExecutionException, InterruptedException 
	{ 
		ExecutorService service = OffHeapCache.getQueryExecutorService();
		int maxWorkers = OffHeapCache.getQueryMaxProcessors();
		LOG.info("Max processors="+maxWorkers);
		
		List<ScanRunner<T>> scanRunners = new ArrayList<ScanRunner<T>>(maxWorkers);
		try{
				
		
			for(int i=0; i < maxWorkers; i++)
			{
				
				Scan<T> scan = (Scan<T>) mScanClass.newInstance();				
				scan.configure(cfg);
				ScanRunner<T> sr = new  ScanRunner<T>( new CacheScanner(cache, i, maxWorkers), mFilter, scan);
				scanRunners.add(sr);
			}
			
			List<Future<T>> futures = service.invokeAll(scanRunners);
			List<T> results = new ArrayList<T>();
			for(Future<T> f: futures){
				results.add(f.get());
			}
			mReduce.configure(cfg);
			return mReduce.reduce(results);
		} catch (InstantiationException e) {

			LOG.error(e);
		} catch (IllegalAccessException e) {

			LOG.error(e);
		}
		
		return null;
	}
	
}
