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
package com.koda.cache.eviction;


import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.koda.IOUtils;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionListener.Reason;
import com.koda.util.SpinLock;

// TODO: Auto-generated Javadoc
/**
 * The Class EvictionAlgo.
 */
public abstract class EvictionAlgo {

	/** The Constant LOG. */
	@SuppressWarnings("unused")
	private final static Logger LOG = Logger.getLogger(EvictionAlgo.class);
	
	/** The parent cache. */

	protected OffHeapCache mParentCache;
	
	/** Eviction listener. */
	protected EvictionListener mEvictionListener ;
	
	/** The m evict on expire first. */
	protected boolean mEvictOnExpireFirst = true;
	
	
	/** The m total eviction attempts. */
	protected AtomicLong mTotalEvictionAttempts = new AtomicLong(0);
	
	/** The m total evicted items. */
	protected AtomicLong mTotalEvictedItems = new AtomicLong(0);
	
	
	/**
	 * Instantiates a new eviction algorithm.
	 *
	 * @param parent the parent
	 */
	public EvictionAlgo(OffHeapCache parent)
	{
		this.mParentCache = parent;
		this.mEvictOnExpireFirst = parent.getEvictOnExpireFirst();
	}
	
	
	/**
	 * Sets the eviction listener.
	 *
	 * @param l the new eviction listener
	 */
	public void setEvictionListener(EvictionListener l)
	{
		this.mEvictionListener = l;
	}
	
	/**
	 * Gets the eviction listener.
	 *
	 * @return the eviction listener
	 */
	public EvictionListener getEvictionListener()
	{
		return mEvictionListener;
	}
	
	/**
	 * Sets the evict on expire first.
	 *
	 * @param value the new evict on expire first
	 */
	public void setEvictOnExpireFirst(boolean value)
	{
		this.mEvictOnExpireFirst = value;
	}
	
	/**
	 * Gets the evict on expire fisrt.
	 *
	 * @return the evict on expire fisrt
	 */
	public boolean getEvictOnExpireFisrt()
	{
		return mEvictOnExpireFirst;
	}
	
	/**
	 * Gets the total eviction attemets.
	 *
	 * @return the total eviction attemets
	 */
	public long getTotalEvictionAttempts()
	{
		return mTotalEvictionAttempts.get();
	}
	
	/**
	 * Gets the total evicted items.
	 *
	 * @return the total evicted items
	 */
	public long getTotalEvictedItems()
	{
		return mTotalEvictedItems.get();
	}
	
	
	public long translate(long data){
		return data;
	}
	
	/**
	 * Inits the entry.
	 *
	 * @param ptr the ptr
	 * @param expire the expire
	 * @param lock the lock
	 */
	public abstract void initEntry(long ptr, long expire, SpinLock lock);
	
	/**
	 * Hit entry.
	 *
	 * @param ptr the ptr
	 * @param lock the lock
	 */
	public abstract void hitEntry(long ptr, SpinLock lock);
	/**
	 * select which one is better for eviction
	 * @param dataFirst  - first record eviction data (not translated)
	 * @param expFirst   - first record expiration time
	 * @param dataSecond - second record eviction data (not translated)
	 * @param expSecond  - second record expiration time
	 * @return 1 if first, 2 if second
	 */
	public abstract int selectBetterCandidate(long dataFirst, long expFirst, long dataSecond, long expSecond);
	
	/**
	 * Do eviction. Finds eviction candidate, removes it physically
	 * from cache, but does not free memory and returns evicted item's
	 * real address.
	 *
	 * @param data the data
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public abstract long doEviction(EvictionData data) throws NativeMemoryException;
	
	public long evict(long index) throws NativeMemoryException
	{
	    mTotalEvictionAttempts.incrementAndGet();
	    
	    long mMemPointer = mParentCache.getMemPointer();
	    long ptr = OffHeapCache.getRealAddress(IOUtils.getLong(mMemPointer, ((long)index) * 8));
	    if(ptr == 0L){
	      return 0;
	    }
	    //LOG.info("evict "+ptr);
	    long nextPtr = mParentCache.getNextAddressRaw(ptr);
	    IOUtils.putLong(mMemPointer, ((long)index) * 8, nextPtr);
	    // Notify listeners
	    if(mEvictionListener != null)
	    {
	      mEvictionListener.evicted(ptr, Reason.ALGO, System.nanoTime());
	    }
	    mTotalEvictedItems.incrementAndGet();
	    mParentCache.decrementCount();

	    return ptr;
	}
	

}
