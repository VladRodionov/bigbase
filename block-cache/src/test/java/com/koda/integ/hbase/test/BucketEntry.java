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
package com.koda.integ.hbase.test;

import java.io.Serializable;


  // TODO: Auto-generated Javadoc
/**
   * The Class BucketEntry.
   */
  public class BucketEntry implements Serializable, Comparable<BucketEntry> {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -6741504807982257534L;
    
    /** The offset base. */
    private int offsetBase;
    
    /** The length. */
    private int length;
    
    /** The offset1. */
    private byte offset1;
    
    /** The deserialiser index. */
    byte deserialiserIndex;
    
    /** The access time. */
    @SuppressWarnings("unused")
    private volatile long accessTime;
    
    /** The priority. */
    private BlockPriority priority;

    /**
     * Instantiates a new bucket entry.
     *
     * @param offset the offset
     * @param length the length
     * @param accessTime the access time
     * @param inMemory the in memory
     */
    BucketEntry(long offset, int length, long accessTime, boolean inMemory) {
      setOffset(offset);
      this.length = length;
      this.accessTime = accessTime;
      if (inMemory) {
        this.priority = BlockPriority.MEMORY;
      } else {
        this.priority = BlockPriority.SINGLE;
      }
    }

    /**
     * Offset.
     *
     * @return the long
     */
    long offset() { // Java has no unsigned numbers
      long o = ((long) offsetBase) & 0xFFFFFFFF;
      o += (((long) (offset1)) & 0xFF) << 32;
      return o << 8;
    }

    /**
     * Sets the offset.
     *
     * @param value the new offset
     */
    private void setOffset(long value) {
      assert (value & 0xFF) == 0;
      value >>= 8;
      offsetBase = (int) value;
      offset1 = (byte) (value >> 32);
    }

    /**
     * Gets the length.
     *
     * @return the length
     */
    public int getLength() {
      return length;
    }

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(BucketEntry arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * Gets the priority.
	 *
	 * @return the priority
	 */
	public BlockPriority getPriority()
	{
		return priority;
	}
}