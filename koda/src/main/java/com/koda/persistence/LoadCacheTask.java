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
package com.koda.persistence;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.koda.cache.OffHeapCache;


// TODO: Auto-generated Javadoc
/**
 * The Class LoadCacheTask.
 */
public class LoadCacheTask implements Future<OffHeapCache>, Runnable
{

	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(LoadCacheTask.class);
	
	/** The store. */
	protected DiskStore store;
	
	/** The pl. */
	protected ProgressListener pl;
	
	/** The cache. */
	protected AtomicReference<OffHeapCache> cache =  
		new AtomicReference<OffHeapCache>(); 
	

	
	/**
	 * Instantiates a new load cache task.
	 *
	 * @param store the store
	 * @param pl the pl
	 */
	public LoadCacheTask(DiskStore store, ProgressListener pl)
	{
		this.store = store;
		this.pl = pl;
	}
	
	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#cancel(boolean)
	 */
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// cancel is not supported
		return false;
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#get()
	 */
	@Override
	public OffHeapCache get() throws InterruptedException, ExecutionException {
		return cache.get();
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public OffHeapCache get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		// timeout is not supported
		return cache.get();
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#isCancelled()
	 */
	@Override
	public boolean isCancelled() {
		// cacncelation is not supported
		return false;
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Future#isDone()
	 */
	@Override
	public boolean isDone() {

		return cache.get() != null;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		try{
			cache.set( store.load( pl));
		}catch(IOException e)
		{
			LOG.error(e);
		}
		
	}
}
