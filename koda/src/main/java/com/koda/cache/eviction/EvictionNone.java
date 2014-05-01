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

import com.koda.cache.OffHeapCache;
import com.koda.util.SpinLock;

// TODO: Auto-generated Javadoc
/**
 * The Class EvictionNone.
 */
public class EvictionNone extends EvictionAlgo {

	/**
	 * Instantiates a new eviction none.
	 *
	 * @param parent the parent
	 */
	public EvictionNone(OffHeapCache parent) {
		super(parent);

	}

	/* (non-Javadoc)
	 * @see com.koda.cache.EvictionAlgo#doEviction(com.koda.cache.EvictionData, int)
	 */
	@Override
	public long doEviction(EvictionData data) {
		// do nothing
		return 0;
	}

	/* (non-Javadoc)
	 * @see com.koda.cache.EvictionAlgo#hitEntry(long, com.koda.util.SpinLock)
	 */
	@Override
	public void hitEntry(long ptr, SpinLock lock) {
		// do nothing

	}

	/* (non-Javadoc)
	 * @see com.koda.cache.EvictionAlgo#initEntry(long, com.koda.util.SpinLock)
	 */
	@Override
	public void initEntry(long ptr, long expire,  SpinLock lock) {
		// do nothing		
	}
  @Override
  public int selectBetterCandidate(long dataFirst, long expFirst,
      long dataSecond, long expSecond) 
  {
    return 1;
  }
}
