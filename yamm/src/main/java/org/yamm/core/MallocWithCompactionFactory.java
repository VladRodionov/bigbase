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
package org.yamm.core;

// TODO: Auto-generated Javadoc
/**
 * A factory for creating MallocWithCompaction objects.
 */
public class MallocWithCompactionFactory implements MallocFactory {

	/** The min slab size. */
	private int minSlabSize = 32;
	
	/** The total slabs. */
	private int totalSlabs = 64;
	
	/** The exp factor. */
	private double expFactor = 1.15D;
	
	/** The max memory. */
	private long maxMemory = 0;// No limit on VM
	
	/**
	 * Instantiates a new malloc with compaction factory.
	 *
	 * @param minSize the min size
	 * @param totalSlabs the total slabs
	 * @param expFactor the exp factor
	 * @param maxMemory the max memory
	 */
	MallocWithCompactionFactory(int minSize, int totalSlabs, double expFactor, long maxMemory){
		this.minSlabSize = minSize;
		this.totalSlabs = totalSlabs;
		this.expFactor = expFactor;
		this.maxMemory = maxMemory;
	}
	
	/**
	 * Instantiates a new malloc with compaction factory.
	 */
	MallocWithCompactionFactory(){}
	
	/**
	 * Gets the single instance of MallocWithCompactionFactory.
	 *
	 * @return single instance of MallocWithCompactionFactory
	 */
	public static MallocFactory getInstance(){
		return new MallocWithCompactionFactory();
	}
	
	/**
	 * Gets the single instance of MallocWithCompactionFactory.
	 *
	 * @param minSize the min size
	 * @param totalSlabs the total slabs
	 * @param expFactor the exp factor
	 * @param maxMemory the max memory
	 * @return single instance of MallocWithCompactionFactory
	 */
	public static MallocFactory getInstance(int minSize, int totalSlabs, double expFactor, long maxMemory){
		return new MallocWithCompactionFactory(minSize, totalSlabs, expFactor, maxMemory);
	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.MallocFactory#newMalloc()
	 */
	@Override
	public Malloc newMalloc() {		
		return new UnsafeMalloc(minSlabSize, totalSlabs, expFactor, maxMemory);
	}

}
