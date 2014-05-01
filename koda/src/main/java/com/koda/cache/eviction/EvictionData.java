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


// TODO: Auto-generated Javadoc
/**
 * The Class EvictionData.
 */
public class EvictionData {

	/** The m bucket indexes. */
	private int[] mBucketIndexes;
	/** The m expire times. */
	private long[] mExpireTimes;
	
	/** The m eviction data. */
	private long[] mEvictionData;
	
	/** The m size. */
	private int mSize ;
	
	/** The m roll index. */
	private int mRollIndex=0;

	
	/** The m stripe number. */
	private int mStripeNumber;
	
	/** The m is initialized. */
	private boolean mIsInitialized = false;
	
	/** The is empty. */
	private boolean isEmpty = true;
	
	/**
	 * Instantiates a new eviction data.
	 *
	 * @param size the size
	 * @param stripeNumber the stripe number
	 */
	public EvictionData(int size, int stripeNumber)
	{

		mBucketIndexes = new int[size];
		mExpireTimes = new long[size];
		mEvictionData = new long[size];
		mSize = size;
		mStripeNumber = stripeNumber;
		for(int i=0; i < size; i++){
			mBucketIndexes[i] = -1;
			mExpireTimes[i] = -1;
			mEvictionData[i] = -1;
		}
	}
	
	
	/**
	 * Checks if is empty.
	 *
	 * @return true, if is empty
	 */
	public boolean isEmpty() {return isEmpty;}
	
	/**
	 * Gets the stripe number.
	 *
	 * @return the stripe number
	 */
	public int getStripeNumber()
	{
		return mStripeNumber;
	}
	
	/**
	 * Gets the candidate prev pointers.
	 *
	 * @return the candidate prev pointers
	 */
	public int[] getCandidateIndexes()
	{
		return mBucketIndexes;
	}
	
	
	/**
	 * Gets the expiration times.
	 *
	 * @return the expiration times
	 */
	public long[] getExpirationTimes()
	{
		return mExpireTimes;
	}
	
	/**
	 * Gets the eviction data.
	 *
	 * @return the eviction data
	 */
	public long[] getEvictionData()
	{
		return mEvictionData;
	}
	
	/**
	 * Gets the size.
	 *
	 * @return the size
	 */
	public int getSize()
	{
		return mSize;
	}
	
	/**
	 * Adds the candidate.
	 * TODO:
	 * 1. Result of operation
	 * 
	 * @param index the index
	 * @param expire the expire
	 * @param evData the ev data
	 */
	public boolean addCandidate(int index, long expire, long evData)
	{

		// search index
		for(int i=0; i < mBucketIndexes.length; i++ ){
			if( mBucketIndexes[i] == index) return false;
		}
		// search first -1
		mRollIndex = searchFirstToOverwrite(mRollIndex);

		mBucketIndexes[mRollIndex] = index;
		mExpireTimes[mRollIndex] = expire;
		mEvictionData[mRollIndex] = evData;
		mRollIndex = (mRollIndex +1)% mSize;
		if(mRollIndex == 0) mIsInitialized = true;
		isEmpty = false;
		return true;
		
	}
	
	private int searchFirstToOverwrite(int startIndex) {
    int start = startIndex;
    for(int i =0; i < mSize; i++){
      if(mBucketIndexes[startIndex] < 0) return startIndex;
      startIndex = (startIndex +1) % mSize;
    }
    return start;
  }





  /**
	 * Checks if is initialized.
	 *
	 * @return true, if is initialized
	 */
	public boolean isInitialized()
	{
		return mIsInitialized;
	}
	
	/**
	 * Reset.
	 *
	 * @param index the index
	 */
	public final void reset(int index)
	{
		if(index < 0 || index >= mSize) return;
		mBucketIndexes[index] = -1;
		mExpireTimes[index] = -1;
		mEvictionData[index] = -1;
	}
}
