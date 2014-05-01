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
package com.koda.integ.hbase.storage;

// TODO: Auto-generated Javadoc
/**
 * The Class TwoStepStorageRecyclerAlgo.
 */
public class TwoStepStorageRecyclerAlgo implements StorageRecyclerAlgo {


	
	/** Maximum read throughput in MB per second. */
	public final static String MAX_READ_THROUGHPUT_CONF   = "mmsra.max.read.throughput";
	
	/** The Constant MIN_READ_THROUGHPUT_CONF. */
	public final static String MIN_READ_THROUGHPUT_CONF   = "mmsra.min.read.throughput";	
	/** Eviction threshold. */
	public final static String EVICTION_MIN_THRESHOLD_CONF    = "mmsra.min.eviction.threshold";
	
	/** The Constant EVICTION_MAX_THRESHOLD_CONF. */
	public final static String EVICTION_MAX_THRESHOLD_CONF    = "mmsra.max.eviction.threshold";	
	/** Active object write batch size. */
	public final static String WRITE_MIN_BATCH_SIZE_CONF      = "mmsra.min.write.batch.size";
	
	/** The Constant WRITE_MAX_BATCH_SIZE_CONF. */
	public final static String WRITE_MAX_BATCH_SIZE_CONF      = "mmsra.max.write.batch.size";	
	
	/** The min read tput. */
	
	/** IO read tput minimum in MBs */
	private int minReadTput;
	
	/** IO read tput maximum in MBs. */
	private int maxReadTput;	
	
	/** Minimum Eviction Threshold. */
	private float minEvictionThreshold;	
	
	/** Maximum Eviction Threshold. */
	private float maxEvictionThreshold;
	
	/** Minimum Write Batch Size. */
	private float minWriteBatchSize;	
	
	/** Maximum Write Batch Size. */
	private float maxWriteBatchSize;	
	
	/**
	 * Instantiates a new two step storage recycler algo.
	 */
	public TwoStepStorageRecyclerAlgo()
	{
		
	}
	
	/**
	 * Gets the min read tput.
	 *
	 * @return the minReadTput
	 */
	public int getMinReadTput() {
		return minReadTput;
	}

	/**
	 * Sets the min read tput.
	 *
	 * @param minReadTput the minReadTput to set
	 */
	public void setMinReadTput(int minReadTput) {
		this.minReadTput = minReadTput;
	}

	/**
	 * Gets the max read tput.
	 *
	 * @return the maxReadTput
	 */
	public int getMaxReadTput() {
		return maxReadTput;
	}

	/**
	 * Sets the max read tput.
	 *
	 * @param maxReadTput the maxReadTput to set
	 */
	public void setMaxReadTput(int maxReadTput) {
		this.maxReadTput = maxReadTput;
	}

	/**
	 * Gets the min eviction threshold.
	 *
	 * @return the minEvictionThreshold
	 */
	public float getMinEvictionThreshold() {
		return minEvictionThreshold;
	}

	/**
	 * Sets the min eviction threshold.
	 *
	 * @param minEvictionThreshold the minEvictionThreshold to set
	 */
	public void setMinEvictionThreshold(float minEvictionThreshold) {
		this.minEvictionThreshold = minEvictionThreshold;
	}

	/**
	 * Gets the max eviction threshold.
	 *
	 * @return the maxEvictionThreshold
	 */
	public float getMaxEvictionThreshold() {
		return maxEvictionThreshold;
	}

	/**
	 * Sets the max eviction threshold.
	 *
	 * @param maxEvictionThreshold the maxEvictionThreshold to set
	 */
	public void setMaxEvictionThreshold(float maxEvictionThreshold) {
		this.maxEvictionThreshold = maxEvictionThreshold;
	}

	/**
	 * Gets the min write batch size.
	 *
	 * @return the minWriteBatchSize
	 */
	public float getMinWriteBatchSize() {
		return minWriteBatchSize;
	}

	/**
	 * Sets the min write batch size.
	 *
	 * @param minWriteBatchSize the minWriteBatchSize to set
	 */
	public void setMinWriteBatchSize(float minWriteBatchSize) {
		this.minWriteBatchSize = minWriteBatchSize;
	}

	/**
	 * Gets the max write batch size.
	 *
	 * @return the maxWriteBatchSize
	 */
	public float getMaxWriteBatchSize() {
		return maxWriteBatchSize;
	}

	/**
	 * Sets the max write batch size.
	 *
	 * @param maxWriteBatchSize the maxWriteBatchSize to set
	 */
	public void setMaxWriteBatchSize(float maxWriteBatchSize) {
		this.maxWriteBatchSize = maxWriteBatchSize;
	}

	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.StorageRecyclerAlgo#adjustRecyclerParameters()
	 */
	@Override
	public void adjustRecyclerParameters() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.StorageRecyclerAlgo#init(com.koda.integ.hbase.storage.StorageRecycler)
	 */
	@Override
	public void init(StorageRecycler storage) {
		// TODO Auto-generated method stub

	}

}
