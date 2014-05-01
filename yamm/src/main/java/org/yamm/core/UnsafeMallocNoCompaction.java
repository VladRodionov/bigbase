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

import java.util.Arrays;

import org.yamm.util.UnsafeWrapper;

import com.koda.common.util.UnsafeAccess;

// TODO: Auto-generated Javadoc
/**
 * YAMM- Yet Another Memory Manager.
 * Slab allocator, fast , not thread-safe. Does not supports slab compaction.
 * 
 * 
 */
public class UnsafeMallocNoCompaction implements Malloc{

	/** The Constant PTR_SIZE. */
	private final static int PTR_SIZE = 8;

	/** The max block size. */
	private final int maxBlockSize = 4*1024 * 1024;

	/** The exp factor. */
	double expFactor = 1.20D;
	
	/** The min size. */
	int minSize = 16;
	
	/** The total slabs. */
	int totalSlabs = 64;
	
	/** The slabs allocd. */
	int slabsAllocd = 0;
	// This gives us max allocation size ~ 12MB
	// We can not allocate more than 2GB
	/** The slab sizes. */
	long[] slabSizes;
	
	/** The free list. */
	final MemoryPointerList[] freeList;
	
	/** The slab start blocks. */
	final long[] slabStartBlocks;
	
	/** The slab current blocks. */
	final long[] slabCurrentBlocks;
	
	/** The cur indexes. */
	final int[] curIndexes;
	
	// Tracks total size of large object allocated (large than slabSizes[totalSlabs-1])
	/** The large object allocated. */
	long largeObjectAllocated = 0;
	
	/** The large object deallocated. */
	long largeObjectDeallocated = 0;
	
	/** The Constant unsafe. */
	final static UnsafeWrapper unsafe = new UnsafeWrapper(UnsafeAccess.getUnsafe());

	
	/**
	 * Maximum amount virtual memory to allocate (0 - no limit)
	 * We impose hard limit.
	 */
	
	private long maxMemory = 0;
	
	/**  When enabled - no allocation can be as since there is no compaction support*/
	private boolean compactionEnabled;

	@SuppressWarnings("unused")
  private Malloc parent;
	
	/**
	 * Instantiates a new unsafe malloc no compaction.
	 */
	public UnsafeMallocNoCompaction() {
		// We allocate twice slab slots to accomodate all possible allocation sizes
		// up to 4GB
		slabSizes = new long[2*totalSlabs];
		freeList = new MemoryPointerList[totalSlabs];
		for (int i = 0; i < totalSlabs; i++)
			freeList[i] = new MemoryPointerList();
		slabStartBlocks = new long[totalSlabs];
		slabCurrentBlocks = new long[totalSlabs];
		curIndexes = new int[totalSlabs];
		//slabsAllocd ++;
		initAll();
	}

	/**
	 * Instantiates a new unsafe malloc no compaction.
	 *
	 * @param maxMemory the max memory
	 */
	public UnsafeMallocNoCompaction(long maxMemory) {
		this();
		this.maxMemory = maxMemory;
	}

	/**
	 * Instantiates a new unsafe malloc no compaction.
	 *
	 * @param minSize the min size
	 * @param totalSlabs the total slabs
	 * @param expFactor the exp factor
	 * @param maxMemory the max memory
	 */
	public UnsafeMallocNoCompaction(int minSize, int totalSlabs, double expFactor,
			long maxMemory) {
		this.minSize = minSize;
		// this.
		slabSizes = new long[2*totalSlabs];
		freeList = new MemoryPointerList[totalSlabs];
		for (int i = 0; i < totalSlabs; i++)
			freeList[i] = new MemoryPointerList();
		slabStartBlocks = new long[totalSlabs];
		slabCurrentBlocks = new long[totalSlabs];
		curIndexes = new int[totalSlabs];
		//slabsAllocd ++;
		this.maxMemory = maxMemory;
		initAll();
	}

	/**
	 * Inits the all.
	 */
	private void initAll() {
		slabSizes[0] = minSize;
		for (int i = 1; i < slabSizes.length; i++) {
			long size = ((long) (slabSizes[i - 1] * expFactor));
			//if( size > Integer.MAX_VALUE -1) {
			//	size = 0;
			//} else{
				size = (size / 8) * 8 + 8;
			//}
			
			slabSizes[i] = size;
			if(i < totalSlabs){
				freeList[i] = new MemoryPointerList();
			}
		}
	}

	/**
	 * Block size for slab id.
	 *
	 * @param id the id
	 * @return the int
	 */
	private final long blockSizeForSlabId(int id) {
		long size = slabSizes[id];
		if (size <= maxBlockSize ) {
			// TODO use maxBlockSize for all slabs below maxBlockSize 
			return maxBlockSize; 
		} else {
			return size + PTR_SIZE;
		}
	}

	/**
	 * Block size for slab.
	 *
	 * @param size the size
	 * @return the int
	 */
	public final long blockSizeForSlab(long size) {
		if (size <= maxBlockSize ) {
			return maxBlockSize; 
		} else {
			return size + PTR_SIZE;
		}
	}

	/**
	 * Max entries per slab id.
	 *
	 * @param id the id
	 * @return the int
	 */
	private final int maxEntriesPerSlabId(int id) {
		long size = slabSizes[id];

		return maxEntriesPerSlab(size);

	}

	/**
	 * Max entries per slab.
	 *
	 * @param size the size
	 * @return the int
	 */
	private final int maxEntriesPerSlab(long size) {
		long blockSize = blockSizeForSlab(size) - PTR_SIZE;
		return (int) (blockSize / size);

	}

	/**
	 * Gets the slab id.
	 *
	 * @param size the size
	 * @return the slab id
	 */
	private int getSlabId(long size) {
		for (int i = 0; i < slabSizes.length; i++) {
			if (slabSizes[i] >= size)
				return i;
		}
		return -1;
	}

	/**
	 * Public API.
	 *
	 * @param size the size
	 * @return the slab size
	 */

	/**
	 * TEST
	 */
	public long getSlabSize(long size) {
		return slabSizes[getSlabId(size)];
	}

	/**
	 * TEST.
	 *
	 * @return the slab sizes
	 */
	
	public long[] getSlabSizes()
	{
		return Arrays.copyOf(slabSizes, slabSizes.length);
	}
	

	
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#malloc(long)
	 */
	@Override
	public long malloc(long size) {

		// DRS support - allocate 8 bytes more
		// to keep back reference to DRS list
		final int slabId = getSlabId(size + PTR_SIZE);
		if( slabId == -1){
			return 0L; // exceeds 2GB
		} else if ( slabId >= totalSlabs){	
				
			// large object requested - allocate it using libc malloc
			// TODO - memory limit check
			if(maxMemory > 0 && (memoryAllocated() + slabSizes[slabId] > maxMemory) ){
				// Exceeds max allowed memory
				return 0L;
			}
			
			long ptr = unsafe.allocateMemory(slabSizes[slabId]);
			if(ptr != 0L){
				largeObjectAllocated += slabSizes[slabId];
			} 
			return slabToPtr(ptr, slabId);			
		}
		
		//final int slotSize = slabSizes[slabId];
		final MemoryPointerList list = freeList[slabId];
		long allocd = list.get();
		if (allocd == 0L) {
			// no available slots - check slab allocation
			// perform compaction (if needed) or allocate new block
			// in a slab (if needed)
			if (checkSlabAllocation(slabId) == false) {
				// malloc failed - return 0L
				return 0L;
			}

			// bump the index
			allocd = slabCurrentBlocks[slabId] + curIndexes[slabId]
					* slabSizes[slabId];
			
			curIndexes[slabId]++;
			allocd = slabToPtr(allocd, slabId);
		}

		return allocd;
	}


	/**
	 * Slab to ptr.
	 *
	 * @param ptr the ptr
	 * @param slabId the slab id
	 * @return the long
	 */
	private final long slabToPtr(long ptr, int slabId) {
		return (((long) slabId) << 48) | ptr;
	}

	/**
	 * Public API: get 'real' address from pointer.
	 *
	 * @param ptr the ptr
	 * @return the long
	 */
	public static long address(long ptr) {
		// return lower 6-bytes - skip first 8 bytes (they hold 'ref')
		return (ptr) & 0xffffffffffffL;
	}
	
	/**
	 * Allocate block.
	 *
	 * @param slabId the slab id
	 * @return the long
	 */
	private long allocateBlock(int slabId){
		long size = blockSizeForSlabId(slabId);
		long ptr = 0L;
		// TODO - this can be costly
		@SuppressWarnings("unused")
    long memUsed = memoryAllocated();
		// We do compaction only when block size is 'maxBlockSize'
		// as since we reassign blocks between slabs and block sizes MUST match
		//if (compactionEnabled || (maxMemory > 0 && (size + memUsed) > maxMemory)) {
		//	return 0L; // Allocation failed			
		//}
		// Allocate new block if we still do not have block (ptr == 0L)
		ptr = unsafe.allocateMemory(size) ;						
		slabsAllocd++;		
		return ptr;
	}

	
	/**
	 * Check slab allocation.
	 *
	 * @param slabId the slab id
	 * @return true, if successful
	 */
	private boolean checkSlabAllocation(int slabId) {

		if (slabCurrentBlocks[slabId] == 0) {
			long ptr = allocateBlock(slabId);
			// Nullify the next block reference
			if(ptr == 0L){
				return false;
			}
			long size = blockSizeForSlabId(slabId);
			unsafe.putLong(ptr + size - PTR_SIZE, 0L);
			slabCurrentBlocks[slabId] = ptr;
			slabStartBlocks[slabId] = ptr;
			curIndexes[slabId] = 0;

		} else {

			int max = maxEntriesPerSlabId(slabId);
			int idx = curIndexes[slabId];
			if (idx == max ) {
				// allocate new
				long size = blockSizeForSlabId(slabId);
				long ptr = allocateBlock(slabId);
				if(ptr == 0L) {
					return false;
				}
				unsafe.putLong(ptr + size - PTR_SIZE, 0L);
				unsafe.putLong(slabCurrentBlocks[slabId] + size - PTR_SIZE, ptr);
				slabCurrentBlocks[slabId] = ptr;
				curIndexes[slabId] = 0;

			}
		}

		return true;

	}


	/**
	 * Total available.
	 *
	 * @return the long
	 */
	public long totalAvailable()
	{
		long total = 0;
		for(int i=0; i < totalSlabs; i++){
			total += freeList[i].size() * slabSizes[i];
		}
		return total;
	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#free(long)
	 */
	@Override
	public void free(long ptr) {
		// Check large object
		int slabId = getSlabIdFromPtr(ptr);
		if(slabId < totalSlabs && slabId >=0){
			MemoryPointerList list = freeList[slabId];
			// Put ptr back to free slot list for a given slab
			list.put(ptr);
		} else{
			// deallocate large object
			unsafe.freeMemory( ptr & 0xffffffffffffL);
			largeObjectDeallocated += slabSizes[slabId];
		}

	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#mallocUsableSize(long)
	 */
	@Override
	public final long mallocUsableSize(long ptr) {
		
		int id = getSlabIdFromPtr(ptr);
		return slabSizes[id];
	}

	/**
	 * Gets the memory block size.
	 *
	 * @return the memory block size
	 */
	public int getMemoryBlockSize() {
		return maxBlockSize;
	}

	/**
	 * Gets the total memory blocks allocated.
	 *
	 * @return the total memory blocks allocated
	 */
	public int getTotalMemoryBlocksAllocated() {
		// TODO - not all blocks are equals=s
		return slabsAllocd;
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#memoryAllocated()
	 */
	@Override
	public long memoryAllocated() {
		long size = 0;
//		long totalBlocks = 0;
//		for (MemoryPointerList list : freeList) {
//			size += list.memoryAllocated();
//			totalBlocks += list.totalBlocks();
//		}
		// TODO: this calculation is based on assumption that all blocks are
		// equal
    long estimatedVMemPart = getEstimatedVMemPart();
		return size + ((long) slabsAllocd) * maxBlockSize +
		  (largeObjectAllocated - largeObjectDeallocated) - totalAvailable() - estimatedVMemPart;
	}
	
  private long memoryAllocatedExact() {
    long size = 0;
//    long totalBlocks = 0;
//    for (MemoryPointerList list : freeList) {
//      size += list.memoryAllocated();
//      totalBlocks += list.totalBlocks();
//    }
//    size += freeDrsList.memoryAllocated();
//    totalBlocks += freeDrsList.totalBlocks();

    
    return size + ((long) slabsAllocd) * maxBlockSize +
      (largeObjectAllocated - largeObjectDeallocated) /*- totalAvailable() */ ;
  }
//  private final long getEstimatedVMemPart() {
//    long estimate = 0;
//    for(int i=0; i < totalSlabs; i++)
//    {
//      if(slabStartBlocks[i] != 0) estimate += maxBlockSize/2;
//      if(freeList[i] != null){
//        estimate +=freeList[i].blockSize() /2;
//      }
//    }
//    return estimate;
//  }
  private final long getEstimatedVMemPart() {
    long estimate = 0;
    for(int i=0; i < totalSlabs; i++)
    {
      if(slabStartBlocks[i] != 0) {
        estimate +=(maxEntriesPerSlabId(i) -  curIndexes[i]) * slabSizes[i];
        
        //estimate += maxBlockSize/2;
      }
      //if(freeList[i] != null){
      //  estimate +=freeList[i].blockSize() /2;
      //}
    }
    return estimate;
  }
	/**
	 * Large memory allocated.
	 *
	 * @return the long
	 */
	public long largeMemoryAllocated()
	{
		return largeObjectAllocated;
	}
	
	/**
	 * Large memory deallocated.
	 *
	 * @return the long
	 */
	public long largeMemoryDeallocated()
	{
		return largeObjectDeallocated;
	}


	/**
	 * Gets the max memory.
	 *
	 * @return the max memory
	 */
	@Override
	public long getMaxMemorySize() {
		return maxMemory;
	}

	@Override
	public void setMaxMemorySize(long size)
	{
		this.maxMemory = size;
	}

	/**
	 * Gets the slab id from ptr.
	 *
	 * @param ptr the ptr
	 * @return the slab id from ptr
	 */
	private int getSlabIdFromPtr(long ptr) {
		int id = (int) (ptr >>> 48) & 0xff;
		return id;
	}

	/**
	 * DEBUG options.
	 */

	public void dumpSlabAllocationSizes() {
		for (int i = 0; i < slabSizes.length; i++) {
			if(slabSizes[i] == 0) break;
			System.out.println(i + ": size=" + slabSizes[i]);
		}
	}

	/**
	 * Free list dump.
	 */
	public void freeListDump() {
		int i = 0;
		for (MemoryPointerList list : freeList) {
			System.out.print((i++) + " ");
			list.dump();
		}

	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#ralloc(long, long)
	 */
	@Override
	public long ralloc(long ptr, long newSize) {
		
    long slabSize = getSlabSize(newSize);
    long useableSize = mallocUsableSize(ptr);
    if(useableSize == slabSize) return ptr;
    
    long allocd = malloc(newSize);
    if( allocd != 0L ) {
      // Free old ptr only if we allocated new one
      free(ptr);
    }
    return allocd;
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#lockAddress(long)
	 */
	@Override
	public final long lockAddress(long ptr) {
		// Does almost nothing as since there is no compaction
		// no locks required
		return ptr & 0xffffffffffffL;
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#unlockAddress(long)
	 */
	@Override
	public final void unlockAddress(long ptr) {
		// Does nothing as since there is no compaction
		
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#isLockEnabled()
	 */
	@Override
	public boolean isLockEnabled() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#setLockEnabled(boolean)
	 */
	@Override
	public void setLockEnabled(boolean b) {
		// DO nothing
		
	}

	@Override
	public long realAddress(long ptr) {
		return ptr & 0xffffffffffffL;
	}

	@Override
	public boolean isCompactionActive() {
		// TODO Auto-generated method stub
		return compactionEnabled || (maxMemory > 0) && maxMemory <= memoryAllocatedExact();
		
	}

	@Override
	public boolean isCompactionSupported() {
		return false;
	}

	@Override
	public void setCompactionActive(boolean b) {		
		if(canControlCompaction())
		compactionEnabled = b;
	}
	
	
	private boolean canControlCompaction()
	{
		return maxMemory == 0;
	}

  @Override
  public void setSlabSizes(long[] sizes) {
    // We expect array of size 2 * totalSlabs
    if(sizes.length != 2 * totalSlabs) return;    
    slabSizes = Arrays.copyOf(sizes, sizes.length);
    minSize = (int)slabSizes[0];
    
  }

  @Override
  public void setParetAllocator(Malloc parent) {
    this.parent = parent;
  }
	
}
