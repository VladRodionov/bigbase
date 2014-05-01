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

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;

import com.google.common.collect.MinMaxPriorityQueue;

// TODO: Auto-generated Javadoc
/**
 * The Class BucketCacheOverhead.
 */
public class BucketCacheOverhead {

/**
 * -XX:+PrintGCApplicationStoppedTime

Hunt, Charlie; John, Binu (2011-10-04). Java Performance (p. 563). Pearson Education (USA). Kindle Edition. 	
 */
	/**
	 * @param args
	 */
	
	static ConcurrentHashMap<BlockCacheKey,BucketEntry> backingMap;
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		backingMap = new 
		ConcurrentHashMap<BlockCacheKey,BucketEntry>();
		int max = 10000000;
		System.out.println("Bucket Cache on-heap overhead per entry calculation");
		Random r = new Random();
		for(int i= 0; i < max; i++){
			BlockCacheKey key = new BlockCacheKey(new String("/hbase/TMO_MAY-UPLOADS/f493283255245c912571355f92b328dd/f/ccbd33f3b8a6452aa6cb64ea9a39f520"),r.nextLong());
			BucketEntry entry = new BucketEntry(10, 10, System.currentTimeMillis(), r.nextBoolean());
			backingMap.put(key, entry);
			
			if(i % 100000 == 0){
				System.out.println("Stored " +  backingMap.size()+" objects.");
			}
		}
		
		long start = System.currentTimeMillis();

		System.out.println("Free space starts: ");
		freeSpace();
		
		System.out.println("Time = "+(System.currentTimeMillis() - start)+" ms");
	}
	
	  /**
  	 * Free space.
  	 */
  	private static void freeSpace() {
		    // Ensure only one freeSpace progress at a time

		    try {
		      long bytesToFreeWithoutExtra = 0;
		      /*
		       * Calculate free byte for each bucketSizeinfo
		       */

		      // Instantiate priority buckets
		      BucketEntryGroup bucketSingle = new BucketEntryGroup(100000000,
		          64*1024, 1000000000);

		      BucketEntryGroup bucketMemory = new BucketEntryGroup(100000000,
		          64*1024, 1000000000);

		      // Scan entire map putting bucket entry into appropriate bucket entry
		      // group
		      for (Map.Entry<BlockCacheKey, BucketEntry> bucketEntryWithKey : backingMap.entrySet()) {
		        switch (bucketEntryWithKey.getValue().getPriority()) {
		          case SINGLE: {
		            bucketSingle.add(bucketEntryWithKey);
		            break;
		          }

		          case MEMORY: {
		            bucketMemory.add(bucketEntryWithKey);
		            break;
		          }
		        }
		      }

		      PriorityQueue<BucketEntryGroup> bucketQueue = new PriorityQueue<BucketEntryGroup>(3);

		      bucketQueue.add(bucketSingle);
		     
		      bucketQueue.add(bucketMemory);

		      int remainingBuckets = 2;
		      long bytesFreed = 0;

		      BucketEntryGroup bucketGroup;
		      while ((bucketGroup = bucketQueue.poll()) != null) {
		        long overflow = bucketGroup.overflow();
		        if (overflow > 0) {
		          long bucketBytesToFree = Math.min(overflow,
		              (bytesToFreeWithoutExtra - bytesFreed) / remainingBuckets);
		          bytesFreed += bucketGroup.free(bucketBytesToFree);
		        }
		        remainingBuckets--;
		      }

		      /**
		       * Check whether need extra free because some bucketSizeinfo still needs
		       * free space
		       */

		      boolean needFreeForExtra = true;
		      long bytesToFreeWithExtra = 2* bytesFreed;
		      if (needFreeForExtra) {
		        bucketQueue.clear();
		        remainingBuckets = 2;

		        bucketQueue.add(bucketSingle);


		        while ((bucketGroup = bucketQueue.poll()) != null) {
		          long bucketBytesToFree = (bytesToFreeWithExtra - bytesFreed)
		              / remainingBuckets;
		          bytesFreed += bucketGroup.free(bucketBytesToFree);
		          remainingBuckets--;
		        }
		      }



		    } finally {

		    }
		  }
	
	

}

class BucketEntryGroup implements Comparable<BucketEntryGroup> {

    private CachedEntryQueue queue;
    private long totalSize = 0;
    private long bucketSize;

    public BucketEntryGroup(long bytesToFree, long blockSize, long bucketSize) {
      this.bucketSize = bucketSize;
      queue = new CachedEntryQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(Map.Entry<BlockCacheKey, BucketEntry> block) {
      totalSize += block.getValue().getLength();
      queue.add(block);
    }

    public long free(long toFree) {
      Map.Entry<BlockCacheKey, BucketEntry> entry;
      long freedBytes = 0;
      while ((entry = queue.pollLast()) != null) {
       // evictBlock(entry.getKey());
        freedBytes += entry.getValue().getLength();
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }
      return freedBytes;
    }

    public long overflow() {
      return totalSize - bucketSize;
    }

    public long totalSize() {
      return totalSize;
    }

    @Override
    public int compareTo(BucketEntryGroup that) {
      if (this.overflow() == that.overflow())
        return 0;
      return this.overflow() > that.overflow() ? 1 : -1;
    }

    @Override
    public boolean equals(Object that) {
      return this == that;
    }

  }
 
class CachedEntryQueue {

	  private MinMaxPriorityQueue<Map.Entry<BlockCacheKey, BucketEntry>> queue;

	  private long cacheSize;
	  private long maxSize;

	  /**
	   * @param maxSize the target size of elements in the queue
	   * @param blockSize expected average size of blocks
	   */
	  public CachedEntryQueue(long maxSize, long blockSize) {
	    int initialSize = (int) (maxSize / blockSize);
	    if (initialSize == 0)
	      initialSize++;
	    queue = MinMaxPriorityQueue
	        .orderedBy(new Comparator<Map.Entry<BlockCacheKey, BucketEntry>>() {
	          public int compare(Entry<BlockCacheKey, BucketEntry> entry1,
	              Entry<BlockCacheKey, BucketEntry> entry2) {
	            return entry1.getValue().compareTo(entry2.getValue());
	          }

	        }).expectedSize(initialSize).create();
	    cacheSize = 0;
	    this.maxSize = maxSize;
	  }

	  /**
	   * Attempt to add the specified entry to this queue.
	   * 
	   * <p>
	   * If the queue is smaller than the max size, or if the specified element is
	   * ordered after the smallest element in the queue, the element will be added
	   * to the queue. Otherwise, there is no side effect of this call.
	   * @param entry a bucket entry with key to try to add to the queue
	   */
	  public void add(Map.Entry<BlockCacheKey, BucketEntry> entry) {
	    if (cacheSize < maxSize) {
	      queue.add(entry);
	      cacheSize += entry.getValue().getLength();
	    } else {
	      BucketEntry head = queue.peek().getValue();
	      if (entry.getValue().compareTo(head) > 0) {
	        cacheSize += entry.getValue().getLength();
	        cacheSize -= head.getLength();
	        if (cacheSize > maxSize) {
	          queue.poll();
	        } else {
	          cacheSize += head.getLength();
	        }
	        queue.add(entry);
	      }
	    }
	  }

	  /**
	   * @return The next element in this queue, or {@code null} if the queue is
	   *         empty.
	   */
	  public Map.Entry<BlockCacheKey, BucketEntry> poll() {
	    return queue.poll();
	  }

	  /**
	   * @return The last element in this queue, or {@code null} if the queue is
	   *         empty.
	   */
	  public Map.Entry<BlockCacheKey, BucketEntry> pollLast() {
	    return queue.pollLast();
	  }

	  /**
	   * Total size of all elements in this queue.
	   * @return size of all elements currently in queue, in bytes
	   */
	  public long cacheSize() {
	    return cacheSize;
	  }
	}
