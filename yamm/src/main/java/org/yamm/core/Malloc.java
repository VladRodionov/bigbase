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
 * The Interface Malloc.
 */
public interface Malloc {
	
	
	
	public void setParetAllocator(Malloc parent);
  /**
	 * Malloc.
	 *
	 * @param size the size
	 * @return the long
	 */
	public long malloc(long size);
	
	/**
	 * Free.
	 *
	 * @param ptr the ptr
	 */
	public void free(long ptr);
	
	/**
	 * Malloc usable size.
	 *
	 * @param ptr the ptr
	 * @return the long
	 */
	public long mallocUsableSize(long ptr);
	
	/**
	 * Memory allocated.
	 *
	 * @return the long
	 */
	public long memoryAllocated();
	
	/**
	 * Ralloc.
	 *
	 * @param ptr the ptr
	 * @param newSize the new size
	 * @return the long
	 */
	public long ralloc(long ptr, long newSize);
	
	/**
	 * Locking supports compaction.
	 *
	 * @param ptr the ptr
	 * @return the long
	 */
	public long lockAddress(long ptr);	
	
	/**
	 * Unlock address.
	 *
	 * @param ptr the ptr
	 */
	public void unlockAddress(long ptr);
	
	/**
	 * Sets the lock enabled.
	 *
	 * @param b the new lock enabled
	 */
	public void setLockEnabled(boolean b);
	
	/**
	 * Checks if is lock enabled.
	 *
	 * @return true, if is lock enabled
	 */
	public boolean isLockEnabled();
	
	public long realAddress(long ptr);
	
	public long getMaxMemorySize();
	
	public void setMaxMemorySize(long size);
	
	public boolean isCompactionSupported();
	
	public void setCompactionActive(boolean b);
	
	public boolean isCompactionActive();
	
	public long[] getSlabSizes();
	
	public void setSlabSizes(long[] sizes);
	
	
	
}
