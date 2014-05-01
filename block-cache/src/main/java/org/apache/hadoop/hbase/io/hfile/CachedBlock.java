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
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

// TODO: Auto-generated Javadoc
/**
 * Represents an entry in the {@link LruBlockCache}.
 *
 * <p>Makes the block memory-aware with {@link HeapSize} and Comparable
 * to sort by access time for the LRU.  It also takes care of priority by
 * either instantiating as in-memory or handling the transition from single
 * to multiple access.
 */
public class CachedBlock implements HeapSize, Comparable<CachedBlock> {

  /** The Constant PER_BLOCK_OVERHEAD. */
  public final static long PER_BLOCK_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (3 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG) +
    ClassSize.STRING + ClassSize.BYTE_BUFFER);

  /**
   * The Enum BlockPriority.
   */
  public static enum BlockPriority {
    
    /** Accessed a single time (used for scan-resistance). */
    SINGLE,
    
    /** Accessed multiple times. */
    MULTI,
    
    /** Block from in-memory store. */
    MEMORY
  };

  /** The cache key. */
  private final BlockCacheKey cacheKey;
  
  /** The buf. */
  private final Cacheable buf;
  
  /** The access time. */
  private volatile long accessTime;
  
  /** The size. */
  private long size;
  
  /** The priority. */
  private BlockPriority priority;

  /**
   * Instantiates a new cached block.
   *
   * @param cacheKey the cache key
   * @param buf the buf
   * @param accessTime the access time
   */
  public CachedBlock(BlockCacheKey cacheKey, Cacheable buf, long accessTime) {
    this(cacheKey, buf, accessTime, false);
  }

  /**
   * Instantiates a new cached block.
   *
   * @param cacheKey the cache key
   * @param buf the buf
   * @param accessTime the access time
   * @param inMemory the in memory
   */
  public CachedBlock(BlockCacheKey cacheKey, Cacheable buf, long accessTime,
      boolean inMemory) {
    this.cacheKey = cacheKey;
    this.buf = buf;
    this.accessTime = accessTime;
    // We approximate the size of this class by the size of its name string
    // plus the size of its byte buffer plus the overhead associated with all
    // the base classes. We also include the base class
    // sizes in the PER_BLOCK_OVERHEAD variable rather than align()ing them with
    // their buffer lengths. This variable is used elsewhere in unit tests.
    this.size = ClassSize.align(cacheKey.heapSize())
        + ClassSize.align(buf.heapSize()) + PER_BLOCK_OVERHEAD;
    if(inMemory) {
      this.priority = BlockPriority.MEMORY;
    } else {
      this.priority = BlockPriority.SINGLE;
    }
  }

  /**
   * Block has been accessed.  Update its local access time.
   *
   * @param accessTime the access time
   */
  public void access(long accessTime) {
    this.accessTime = accessTime;
    if(this.priority == BlockPriority.SINGLE) {
      this.priority = BlockPriority.MULTI;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hbase.io.HeapSize#heapSize()
   */
  public long heapSize() {
    return size;
  }

  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(CachedBlock that) {
    if(this.accessTime == that.accessTime) return 0;
    return this.accessTime < that.accessTime ? 1 : -1;
  }

  /**
   * Gets the buffer.
   *
   * @return the buffer
   */
  public Cacheable getBuffer() {
    return this.buf;
  }

  /**
   * Gets the cache key.
   *
   * @return the cache key
   */
  public BlockCacheKey getCacheKey() {
    return this.cacheKey;
  }

  /**
   * Gets the priority.
   *
   * @return the priority
   */
  public BlockPriority getPriority() {
    return this.priority;
  }
}
