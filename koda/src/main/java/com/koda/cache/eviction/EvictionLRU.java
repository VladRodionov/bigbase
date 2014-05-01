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

import org.apache.log4j.Logger;

import com.koda.IOUtils;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionListener.Reason;
import com.koda.util.SpinLock;

// TODO: Auto-generated Javadoc
/**
 * The Class EvictionLRU. Least-Recently-Used
 */
public class EvictionLRU extends EvictionAlgo {
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(EvictionLRU.class);
	/**
	 * Instantiates a new LRU policy.
	 *
	 * @param parent the parent
	 */
	public EvictionLRU(OffHeapCache parent) {
		super(parent);
		
	}

	/* (non-Javadoc)
	 * @see com.koda.cache.EvictionAlgo#doEviction(com.koda.cache.EvictionData, int)
	 */
	@Override
	public long doEviction(EvictionData data) throws NativeMemoryException {
		
		// 1 first check expired
		long start = mParentCache.getEpochStartTime();
		long current = (System.currentTimeMillis() - start)/1000;

		long evictedPtr = 0;

		long[] expTimes = data.getExpirationTimes();
		
		if(mEvictOnExpireFirst){
			for(int i=0; i < expTimes.length; i++)
			{

				if(expTimes[i] < 0) continue;
				
				if(expTimes[i] != OffHeapCache.NO_EXPIRE && expTimes[i] < current)
				{
					evictedPtr = evict(data, i, Reason.EXPIRED);
					if(evictedPtr == 0L) continue;
					mParentCache.decrementCount();
					// Reset index
					data.reset(i);					
					return evictedPtr;
				}

			}
		}
		
		

		long[] evictionData = data.getEvictionData();
	
		int toEvict=0;
		//find min and evict

		while(evictedPtr == 0L){

			long min = Long.MAX_VALUE;
			int zero = 0;
			for(int i=0; i < evictionData.length; i++ )
			{
				if(evictionData[i] < 0) {
				  zero++;
				  continue;
				}
				if(evictionData[i] < min){
					min = evictionData[i];
					toEvict = i;
				}
			}
//			Random r = new Random();
//			int i = r.nextInt(100000);
//			if( i == 0){
//			  LOG.info("Found "+(System.currentTimeMillis() - (start + min * 100))+" ms. data size="+evictionData.length+" NULLS="+zero);
//			  if(System.currentTimeMillis() - (start + min * 100) < 1000){
//			    // dump all
//			    for(int k=0; k < evictionData.length; k++){
//			      System.out.print(System.currentTimeMillis() - (start + evictionData[k] * 100)+ "["+data.getCandidateIndexes()[k]+"]");
//			    }
//			    System.out.println();
//			  }
//			}
			evictedPtr = evict(data, toEvict, Reason.ALGO);	
			if(evictedPtr != 0) {
				// Reset index
				data.reset(toEvict);
			} else{
				LOG.debug("Nothing to evict");
				return 0;
			}
		}
		
		mParentCache.decrementCount();
		
		return evictedPtr;
	}

	/**
	 * Evicts cache item. Return's item address
	 *
	 * @param data the data
	 * @param i the i
	 * @param reason the reason
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	private long evict(EvictionData data, int i, Reason reason) throws NativeMemoryException {

		mTotalEvictionAttempts.incrementAndGet();
		
		int index = data.getCandidateIndexes()[i];
		if(index < 0){
			data.getEvictionData()[i] = -1;
			return 0;
		}
		long mMemPointer = mParentCache.getMemPointer();
		long ptr = OffHeapCache.getRealAddress(IOUtils.getLong(mMemPointer, ((long)index) * 8));
		if(ptr == 0L){
			data.getEvictionData()[i] = -1;
			return 0;
		}

		long nextPtr = mParentCache.getNextAddressRaw(ptr);

		IOUtils.putLong(mMemPointer, ((long)index) * 8, nextPtr);
		// Notify listeners
	    if(mEvictionListener != null)
	    {
	    	mEvictionListener.evicted(ptr, reason, System.nanoTime());
	    }
	    mTotalEvictedItems.incrementAndGet();
		return ptr;
		
	}


	/* (non-Javadoc)
	 * @see com.koda.cache.EvictionAlgo#hitEntry(long, com.koda.util.SpinLock)
	 */
	@Override
	public void hitEntry(long ptr, SpinLock lock) {
		// lock can be NULL
		long start = mParentCache.getEpochStartTime();
		// We keep track of ts in 1/10th sec 
		long msecs = (System.currentTimeMillis() - start)/100;
		//long msecs = (System.currentTimeMillis() - start);
		
		// We do not do locking in LRU
		IOUtils.putUInt(ptr, 4, msecs);
		
	}

	/* (non-Javadoc)
	 * @see com.koda.cache.EvictionAlgo#initEntry(long, com.koda.util.SpinLock)
	 */
	@Override
	public void initEntry(long ptr, long expire, SpinLock lock) {
		long start = mParentCache.getEpochStartTime();
		long msecs = (System.currentTimeMillis() - start)/100;		
		//long msecs = (System.currentTimeMillis() - start);
		// set expiration time only if expire > 0
		if( expire > 0){
		  IOUtils.putUInt(ptr, 0, (expire-start)/1000);// 1 sec resolution
		} else if( expire == OffHeapCache.NO_EXPIRE ){
		  IOUtils.putUInt(ptr, 0, OffHeapCache.NO_EXPIRE);
		} else if (expire == OffHeapCache.IMMORTAL){
		  IOUtils.putInt(ptr, 0, OffHeapCache.IMMORTAL);
		}
		// We do not do locking in LRU		
		IOUtils.putUInt(ptr, 4, msecs);

	}

	/* (non-Javadoc)
	 * Translates RAW format in cache record to regular value (Time of a last update)
	 * @see com.koda.cache.eviction.EvictionAlgo#translate(long)
	 */
	@Override
	public long translate(long data) {
		long start = mParentCache.getEpochStartTime();
		//LOG.info("data="+data+" epoch="+start);
		return data*100 + start;	
		//return data + start;  
	}


  @Override
  public int selectBetterCandidate(long dataFirst, long expFirst,
      long dataSecond, long expSecond) 
  {
    if(dataFirst == -1) return 2;
    if(mEvictOnExpireFirst == true){
      long start = mParentCache.getEpochStartTime();
      long current = (System.currentTimeMillis() - start)/1000;      
      if(expFirst > OffHeapCache.NO_EXPIRE && expFirst < current){
        return 1; // first
      }
      if(expSecond > OffHeapCache.NO_EXPIRE && expSecond < current){
        return 2; // first
      }
    }

    return (dataFirst < dataSecond)? 1: 2;
  }
	
}
