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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.yamm.util.UnsafeWrapper;

import com.koda.common.util.UnsafeAccess;

// TODO: Auto-generated Javadoc
/**
 * YAMM- Yet Another Memory Manager.
 * Slab allocator, fast , not thread-safe. Supports slab compaction.
 * 
 * 
 * TODO:
 * 
 * + 1) Double-register addressing scheme to support compaction 
 * + 1.5) Add 8 -byte ref of DRS into all allocated object
 * 
 * + 2) Slab compaction - blocking op 
 * + 3) malloc: compaction when no free blocks in a slab 
 * + 4) Add max memory limit 
 * + 5) Add compaction On/Off threshold
 *   6) support for large allocation
 */
public class UnsafeMalloc implements Malloc{

	/** The Constant PTR_SIZE. */
	private final static int PTR_SIZE = 8;

	/** The max block size. */
	private final int maxBlockSize = 4* 1024 * 1024;

	/** The exp factor. */
	double expFactor = 1.20D;
	
	/** The min size. */
	int minSize = 16;
	
	/** The total slabs. */
	int totalSlabs = 64;
	
	/** Slab locks. */
	final ReentrantReadWriteLock[] lockArray;
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
	
	/** Maximum slabId which allocates common block of memory of size 'maxBlockSize'. */
	int maxCommonBlockSlabId;

	/**
	 * Double - register addressing scheme support (DRS) 
	 * drsStartBlock - DRS
	 * list of references start address (1st block) 
	 * 
	 * drsCurrentBlock - DRS list
	 * of references current block (last/top block) 
	 * 
	 * drsCurrentIndex - DRS list
	 * of references write index in top block 
	 * 
	 * freeDrsList - list of free
	 * reference slots (8 bytes each) in DRS list
	 * 
	 * DRS adds approximately 16+ bytes overhead per object. 8 byte reference we
	 * keep in DRS list of references and 8 byte DRS reference we need to keep
	 * with every allocated object to allow update of DRS list during
	 * compaction.
	 */
	final long drsStartBlock;
	
	/** The drs current block. */
	long drsCurrentBlock;
	
	/** The drs current index. */
	int drsCurrentIndex;

	/** The free drs list. */
	final MemoryPointerList freeDrsList;

	/** The Constant unsafe. */
	final static UnsafeWrapper unsafe = new UnsafeWrapper(UnsafeAccess.getUnsafe());

	/** Compaction high / low watermarks. */
	//private float compactionOn = 0.90f;

	/** The compaction off. */
	//private float compactionOff = 0.8f;

	/** Is compaction currently On (enabled). */
	private boolean compactionEnabled = false;
	
	/** Maximum amount virtual memory to allocate (0 - no limit) When there is no limit - there is no compaction. */
	
	private long maxMemory = 0;

	//private long memoryAllocated;
	
	/** The lock enabled. */
	private boolean lockEnabled = true;
	
	@SuppressWarnings("unused")
  private Malloc parent;
	
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
	/**
	 * Instantiates a new unsafe malloc.
	 */
	public UnsafeMalloc() {
		// We allocate twice slab slots to accomodate all possible allocation sizes
		// up to 4GB
		slabSizes = new long[ 2*totalSlabs];
		freeList = new MemoryPointerList[totalSlabs];
		lockArray = new ReentrantReadWriteLock[totalSlabs];
		for (int i = 0; i < totalSlabs; i++)
			freeList[i] = new MemoryPointerList();
		slabStartBlocks = new long[totalSlabs];
		slabCurrentBlocks = new long[totalSlabs];
		curIndexes = new int[totalSlabs];

		// Initialize double-register addressing
		drsStartBlock = unsafe.allocateMemory(maxBlockSize);
		unsafe.putLong(drsStartBlock + maxBlockSize - PTR_SIZE, 0L);
		drsCurrentBlock = drsStartBlock;
		//slabsAllocd ++;
		freeDrsList = new MemoryPointerList();

		initAll();
	}

	/**
	 * Instantiates a new unsafe malloc.
	 *
	 * @param maxMemory the max memory
	 */
	public UnsafeMalloc(long maxMemory) {
		this();
		this.maxMemory = maxMemory;
	}

	/**
	 * Instantiates a new unsafe malloc.
	 *
	 * @param minSize the min size
	 * @param totalSlabs the total slabs
	 * @param expFactor the exp factor
	 * @param maxMemory the max memory
	 */
	public UnsafeMalloc(int minSize, int totalSlabs, double expFactor,
			long maxMemory) {
		this.minSize = minSize;
		this.totalSlabs = totalSlabs;
		this.expFactor = expFactor;
		// this.
		slabSizes = new long[2*totalSlabs];
		freeList = new MemoryPointerList[totalSlabs];
		lockArray = new ReentrantReadWriteLock[totalSlabs];
		for (int i = 0; i < totalSlabs; i++)
			freeList[i] = new MemoryPointerList();
		slabStartBlocks = new long[totalSlabs];
		slabCurrentBlocks = new long[totalSlabs];
		curIndexes = new int[totalSlabs];
		// Initialize double-register addressing
		drsStartBlock = unsafe.allocateMemory(maxBlockSize);
		unsafe.putLong(drsStartBlock + maxBlockSize - PTR_SIZE, 0L);
		drsCurrentBlock = drsStartBlock;
		//slabsAllocd ++;
		freeDrsList = new MemoryPointerList();
		this.maxMemory = maxMemory;
		initAll();
	}

	/**
	 * Inits the all.
	 */
	private void initAll() {
		slabSizes[0] = minSize;
		freeList[0] = new MemoryPointerList();
		lockArray[0] = new ReentrantReadWriteLock();
		for (int i = 1; i < slabSizes.length; i++) {
		  //if (i == 49) expFactor = 1.20;
		  //if( i == 56) expFactor = 1.25;
			//if( i == 64) expFactor = 1.15;
		  long size = ((long) (slabSizes[i - 1] * expFactor));
			size = (size / 8) * 8 + 8;			
			slabSizes[i] =  size;
			if(slabSizes[i] <= maxBlockSize && i < totalSlabs){
				maxCommonBlockSlabId = i;
			}
			if(i < totalSlabs){
				freeList[i] = new MemoryPointerList();
				lockArray[i] = new ReentrantReadWriteLock();
			}
		}
		
		//dumpSlabAllocationSizes();
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
		return (int)(blockSize / size);

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
	
	/**
	 * DEBUG.
	 *
	 * @param addr the addr
	 */
	private void findBlock(long addr)
	{
		for(int i=0 ; i < slabStartBlocks.length; i++){
			long ptr = slabStartBlocks[i];
			System.out.println("\n"+ i + " start " + ptr);
			while(ptr != 0){
				if( ptr == addr){
					System.out.println("found in slab "+ i);
				}
				ptr = unsafe.getLong(ptr + maxBlockSize - PTR_SIZE);
				System.out.print(" "+ptr);
			}
			
		}
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
				
			if( isCompactionActive()) {
				// we can not compact large blocks - allocation failed
				return 0L;
			}
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

		long realAddress = allocd & 0xffffffffffffL;
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
			// put slabId into pointer
			realAddress = allocd;
			allocd = slabToPtr(allocd, slabId);
		}
		// TODO NULL checks
		// now we have allocation slot
		// do DRS addressing
		long ref = getDRSRef(allocd);
		// Write 'ref' back into 'allocd'
		//DEBUG
		if(ref == 0L) return 0L;
				
		unsafe.putLong(realAddress /*+ slotSize - PTR_SIZE*/, ref);
		// check previous
	
		// Return DRS reference
		//System.out.println("MALLOC: "+size+" PTR="+Long.toString(ref, 16)+" REAL="+Long.toString(allocd, 16));
		return ref;
	}

	/**
	 * Gets the dRS ref.
	 *
	 * @param allocd the allocd
	 * @return the dRS ref
	 */
	private long getDRSRef(long allocd) {
		// Check freeList
		long mem = freeDrsList.get();
		if (mem == 0L) {
			if(checkDRSAllocation() == false){
				return 0L;
			}
			mem = drsCurrentBlock + (drsCurrentIndex++ * PTR_SIZE);
		}
		unsafe.putLong(mem, allocd);
		return mem;
	}

	/**
	 * TODO: compaction.
	 */
	private boolean checkDRSAllocation() {

		int max = (maxBlockSize - PTR_SIZE) / PTR_SIZE;
		int idx = drsCurrentIndex;
		if (idx == max ) {
			// allocate new
			int size = maxBlockSize;
			long ptr = allocateStandardBlock();//unsafe.allocateMemory(size);
			if( ptr == 0) return false;
			unsafe.putLong(ptr + size - PTR_SIZE, 0L);
			unsafe.putLong(drsCurrentBlock + size - PTR_SIZE, ptr);
			drsCurrentBlock = ptr;
			drsCurrentIndex = 0;
			//slabsAllocd++;
			
		}
		return true;
	}


	/**
	 * Slab to ptr.
	 *
	 * @param ptr the ptr
	 * @param slabId the slab id
	 * @return the long
	 */
	private final long slabToPtr(long ptr, int slabId) {
		return ((long) (slabId + 1)) << 48 | ptr;
	}

	/**
	 * Public API: get 'real' address from pointer.
	 *
	 * @param mem the mem
	 * @return the long
	 */
	public static long address(long mem) {
		// DRaS double-registry allocation scheme
		long ptr = unsafe.getLong(mem);
		// return lower 6-bytes - skip firts 8 bytes (they hold 'ref')
		return (ptr + PTR_SIZE) & 0xffffffffffffL;
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
		// Check compaction first
		// TODO - this can be costly
//		long memUsed = memoryAllocated();
		// We do compaction only when block size is 'maxBlockSize'
		// as since we reassign blocks between slabs and block sizes MUST match
//		if (maxMemory > 0 && size == maxBlockSize) {

//			if (memUsed + size < compactionOff * maxMemory) {
//				compactionEnabled = false;
//			} else if (memUsed + size > compactionOn * maxMemory) {
//				compactionEnabled = true;
//			}
			// We can compact only if request slabId <= maxCommonBlockSlab
			if (isCompactionActive() && slabId <= maxCommonBlockSlabId) {
				// do compaction
				ptr = compact();
				// ptr == 0L when we fail to compact heap
				if (ptr == 0L /*&& memUsed + size > maxMemory*/) {
					// Allocation failed
					// Do nothing
					return 0L;
				}
			}
//		}


		boolean recycled = ptr != 0L;

		// Allocate new block if we still do not have block (ptr == 0L)
		ptr = ptr == 0L ? unsafe.allocateMemory(size) : ptr;
				
		if (recycled == false){
			slabsAllocd++;
		} else{
			recycledBlocks++;
		}
		
		return ptr;
	}

	/**
	 * Allocate standard block.
	 *
	 * @return the long
	 */
	private long allocateStandardBlock(){
		int size = maxBlockSize;
		long ptr = 0L;
		// Check compaction first
		// TODO - this can be costly
//		long memUsed = memoryAllocated();
		// We do compaction only when block size is 'maxBlockSize'
		// as since we reassign blocks between slabs and block sizes MUST match
//		if (maxMemory > 0 && size == maxBlockSize) {

//			if (memUsed + size < compactionOff * maxMemory) {
//				compactionEnabled = false;
//			} else if (memUsed + size > compactionOn * maxMemory) {
//				compactionEnabled = true;
//			}
			// We can compact only if request slabId <= maxCommonBlockSlab
			if ( isCompactionActive() ) {
				// do compaction
				ptr = compact();
				// ptr == 0L when we fail to compact heap
				if (ptr == 0L /*&& memUsed + size > maxMemory*/) {
					// Allocation failed
					// Do nothing
					return 0L;
				}
			}
//		}


		boolean recycled = ptr != 0L;

		// Allocate new block if we still do not have block (ptr == 0L)
		ptr = ptr == 0L ? unsafe.allocateMemory(size) : ptr;
				
		if (recycled == false){
			slabsAllocd++;
		} else{
			recycledBlocks++;
		}
		
		return ptr;
	}

	
	/**
	 * Gets the recycled blocks.
	 *
	 * @return the recycled blocks
	 */
	public long getRecycledBlocks()
	{
		return recycledBlocks;
	}
	
	/** The recycled blocks. */
	private long recycledBlocks =0;
	
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
	 * Compact.
	 * 1. Check if the current slab is being compacted
	 * 
	 * @return the long
	 */
	private long compact() {
		
		if(System.getProperty("compaction.disabled")!= null) return 0L;

		
		// 1) Start scanning slabs from 'maxCommonBlockSlubId' to 0
		//   Find first slab which has more than maxBlockSize storage available
		//int found = -1;
		for(int i = maxCommonBlockSlabId ; i >=0 ; i--){
			final MemoryPointerList list = freeList[i];
			// slab object size
			//final int size = slabSizes[i];
			final int maxPerBlock = maxEntriesPerSlabId(i);
			// NO NEED to lock 
			//writeLockSlab(i);
			boolean locked = false;
			try{
			  if(list.size() >= maxPerBlock){
			    // Ok, we found slab with enough available memory
					//  found = i; break;
			    //writeLockSlab(i);
			    lock.writeLock().lock();
			    locked = true;
			    return compactSlab(i);
			  }
			} finally{
			  if(locked)  lock.writeLock().unlock();//writeUnlockSlab(i);
			}
		}
		
		//if(found >= 0) {
		//	return compactSlab(found);
		//} else{
			// Nothing to compact
			return 0L;
		//}
	}

	
	/**
	 * Compact slab.
	 *
	 * @param id the id
	 * @return the long
	 */
	private long compactSlab(int id) {
		
		long start = System.nanoTime();
		
		final int maxSlotsToCompact = maxEntriesPerSlabId(id);
		final long slotSize = slabSizes[id];
		MemoryPointerList list = freeList[id];
		// TODO : reuse array
		// we allocate twice the space to accommodate free slots from a block
		// we want to compact (there are maxSlotsToCompact of them at max)
		
		// THIS DEBUG - in production we need more scalable approach
		final long[] pointers = new long[(int)list.size()];
		
		// 1. pick up the block to compact - first one
		// this is assumption that the oldest allocated block has the maximum free slots
		final long blockAddr = slabStartBlocks[id];
		boolean singleBlock = slabStartBlocks[id] == slabCurrentBlocks[id];
		
		// 2. set next block as first one
		slabStartBlocks[id] = unsafe.getLong( blockAddr + maxBlockSize - PTR_SIZE);
		if(singleBlock) {
			// TODO single block case
			//System.out.println("SINGLE BLOCK: "+ blockAddr);
			//System.exit(0);
			slabCurrentBlocks[id] = slabStartBlocks[id];
			curIndexes[id] = 0;
		}
		int index = 0;
		int inBlock = 0;
		
		// 3. Calculate size of array to sort and deallocate - totalToProcess
		//while(index < maxSlotsToCompact){
		while(index < pointers.length){
			// this one contains slabId
			long ptr = list.get();
			if( ptr == 0L) break;
			// this one is pure address
			ptr = ptr & 0xffffffffffffL;

			pointers[index++] = ptr;	
			if( ptr >= blockAddr && ptr < blockAddr + maxBlockSize){
			// free slot is inside the block we are cleaning - add
				inBlock++;
			}		
		}

		if(inBlock == maxSlotsToCompact) {
			// All the block is empty - return now.
			// return ALL out of block ptrs back to freeList
			for(int i=0; i < pointers.length; i++){
				if(pointers[i] != 0 && 
						( pointers[i] < blockAddr || pointers[i] >= blockAddr + maxBlockSize)){
					list.put(slabToPtr(pointers[i], id));
				}
			}
			return blockAddr;
		}
		
		//System.out.println("yep - we here");
		//Arrays.sort(pointers, 0, maxSlotsToCompact);
		Arrays.sort(pointers, 0, pointers.length);
		
		// A. Find free slots in block being compacted and mark them
		// B. Find objects from block being compacted in 'pointers'
		long address = blockAddr;
		int idx = 0;
		while(address <= blockAddr + maxBlockSize - PTR_SIZE -slotSize){
			//for(int i= idx; i < maxSlotsToCompact; i++){
			for(int i= idx; i < pointers.length; i++){
				if(pointers[i] >= address){
					idx = i; break;
				}
			}
			if(pointers[idx] == address){
				// Null 'ref' 
				unsafe.putLong(address /*+ slotSize - PTR_SIZE*/, 0L);
				// Null ptr
				pointers[idx] = 0L;
			} else if(pointers[idx] > blockAddr + maxBlockSize){
				break;
			}
			address += slotSize;
		}

		
		// 5. Do compaction
		// The index in pointers array with a current move address
		// [0, totalToProcess[
		int moveIndex = 0;
		// The current index in pointer array to check current slot in a block
		// is free or not (do we have to move it?)
		//

		long addr = blockAddr;
		//int k =0 , reassigned=0;
		while(addr <= blockAddr + maxBlockSize - PTR_SIZE -slotSize){

			//System.out.print( " a1:"+ (addr + slotSize -PTR_SIZE) + " ");
			if(unsafe.getLong(addr /*+ slotSize - PTR_SIZE*/) == 0L) {
				addr+= slotSize;  continue;
			}
			
			if(pointers[moveIndex] == 0L){
				moveIndex = findNextMoveIndex(pointers, maxSlotsToCompact, moveIndex);

			}
			// move
			// Get 'ref' from current slot
			long ref = unsafe.getLong(addr /*+ slotSize - PTR_SIZE*/);
			// Write new location of an object into ref
			// DEBUG
			if(ref > 0xffffffffffffL || ref == 0L){
				System.out.println( " REF:"+ (ref) + " addr ="+blockAddr +" slot="+ id );
				for(int i=0; i < maxSlotsToCompact; i++){
					System.out.println(i+" REF = "+Long.toString(unsafe.getLong(blockAddr + i * slotSize /*- PTR_SIZE*/), 16));
				}
				
				findBlock(blockAddr);
				
				//System.exit(0);
				throw new RuntimeException("UnsafeMalloc: failed compaction.");
			}
			
			long mem = unsafe.getLong(ref);
			if( addr != (mem & 0xffffffffffffL)){
				//reassigned++;
				//System.out.println(reassigned+": Reassigned REF = "+ ref);
				// Was reassigned => we skip this object
				addr += slotSize; continue;
			}
			
			unsafe.putLong(ref, slabToPtr(pointers[moveIndex], id));
			// Copy object to new location
			//System.out.print( " a3:"+ (addr) +" a4:"+(pointers[moveIndex])+ " ");
			unsafe.copyMemory(addr, pointers[moveIndex], slotSize);
			
			addr += slotSize;
			// set 0 for debug
			pointers[moveIndex] = 0L;
			moveIndex++; 
		}		

		int total = 0;
 
		for(int i=0; i < pointers.length; i++){
			if(pointers[i] != 0L){ total ++;
				//System.out.println(total + ": Return back: "+pointers[i]);
				// PUT unassigned slots back to freeList
				list.put(slabToPtr(pointers[i], id));
			}
		}
		/*DEBUG*/System.out.println("[UnsafeMalloc] compaction ="+(System.nanoTime() -start)/1000+" microsec");
		return blockAddr;

	}
	
	
	/**
	 * Find next move index.
	 *
	 * @param pointers the pointers
	 * @param size the size
	 * @param moveIndex the move index
	 * @return the int
	 */
	private int findNextMoveIndex(final long[] pointers, final int size, final int moveIndex) {
		
		for(int i = moveIndex +1; i < size; i++){
			if(pointers[i] != 0L) return i;
		}
		return -1;
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
	public void free(long mem) {
		// DRS support (double register allocation scheme)
		// get real pointer from 'mem'

		lock.readLock().lock();
		try{
		// Check large object
		if( (mem & 0xffffffffffffL) != mem){
			int slabId = getSlabIdFromPtr(mem);//(int) (mem >>> 48 ) & 0xff;
			// deallocate large object
			unsafe.freeMemory( mem & 0xffffffffffffL);
			largeObjectDeallocated += slabSizes[slabId];
			return;
		}
		
		long ptr = unsafe.getLong(mem);
		int slabId = getSlabIdFromPtr(ptr);
		MemoryPointerList list = freeList[slabId];
		// Put ptr back to free slot list for a given slab
		list.put(ptr);
		// add 'mem' to freeDrsList
		freeDrsList.put(mem);
		} finally{
		  lock.readLock().unlock();
		}
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#mallocUsableSize(long)
	 */
	@Override
	public final long mallocUsableSize(long mem) {
		
		// DRas support
		long ptr = 0L;
		int id = 0;
		if( (mem & 0xffffffffffffL) == mem){
			ptr = unsafe.getLong(mem);
			id  = getSlabIdFromPtr(ptr);
		} else{
			ptr = mem & 0xffffffffffffL;
			id = getSlabIdFromPtr(mem);
		}
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
//		size += freeDrsList.memoryAllocated();
//		totalBlocks += freeDrsList.totalBlocks();
		// TODO: this calculation is based on assumption that all blocks are
		// equal
		long estimatedVMemPart = getEstimatedVMemPart();
		
		return size + ((long) slabsAllocd) * maxBlockSize +
		  (largeObjectAllocated - largeObjectDeallocated) - totalAvailable() - estimatedVMemPart;
	}
	

	  private long memoryAllocatedExact() {
	    long size = 0;
//	    long totalBlocks = 0;
//	    for (MemoryPointerList list : freeList) {
//	      size += list.memoryAllocated();
//	      totalBlocks += list.totalBlocks();
//	    }
//	    size += freeDrsList.memoryAllocated();
//	    totalBlocks += freeDrsList.totalBlocks();

	    
	    return size + ((long) slabsAllocd) * maxBlockSize +
	      (largeObjectAllocated - largeObjectDeallocated) /*- totalAvailable() */ ;
	  }
	
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

//	/**
//	 * Gets the compaction on.
//	 *
//	 * @return the compaction on
//	 */
//	public float getCompactionOn() {
//		return compactionOn;
//	}
//
//	/**
//	 * Sets the compaction on.
//	 *
//	 * @param compactionOn the new compaction on
//	 */
//	public void setCompactionOn(float compactionOn) {
//		this.compactionOn = compactionOn;
//	}
//
//	/**
//	 * Gets the compaction off.
//	 *
//	 * @return the compaction off
//	 */
//	public float getCompactionOff() {
//		return compactionOff;
//	}
//
//	/**
//	 * Sets the compaction off.
//	 *
//	 * @param compactionOff the new compaction off
//	 */
//	public void setCompactionOff(float compactionOff) {
//		this.compactionOff = compactionOff;
//	}

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
	public void setMaxMemorySize(long maxMemory) {
		maxMemory = (maxMemory / maxBlockSize + 1) * maxBlockSize;
	  this.maxMemory = maxMemory;
	}

	/**
	 * Gets the slab id from ptr.
	 *
	 * @param ptr the ptr
	 * @return the slab id from ptr
	 */
	private final int getSlabIdFromPtr(long ptr) {
		int id = ((int) (ptr >>> 48) & 0xff) -1;
		return id;
	}

	/**
	 * DEBUG options.
	 */

	public void dumpSlabAllocationSizes() {
		for (int i = 0; i < slabSizes.length; i++) {
			if(slabSizes[i] == 0) break;
			System.out.println(slabSizes[i]);
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
  public final long lockAddress(long mem) {
    // I think its safe because op is atomic
    // and slabId is never changed
    // System.out.println("lock address "+mem);
    
	  lock.readLock().lock();
	  try{
    long ptr = 0;
    int slabId = 0;
    if ((mem & 0xffffffffffffL) == mem) {
      ptr = unsafe.getLong(mem);
      slabId = getSlabIdFromPtr(ptr);
      // We need to lock slab to prevent compaction
      if (isLockEnabled()) {
        readLockSlab(slabId);
      }
      ptr = unsafe.getLong(mem);
    } else {
      ptr = mem & 0xffffffffffffL;
      
    }

    // Returns real address
    return (ptr + PTR_SIZE) & 0xffffffffffffL;
	  } finally{
	    lock.readLock().unlock();
	  }
  }

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#unlockAddress(long)
	 */
	@Override
  public final void unlockAddress(long mem) {
    lock.readLock().lock();
    try{
	  if (isLockEnabled()) {
      long ptr = 0;
      int slabId = 0;
      if ((mem & 0xffffffffffffL) == mem) {
        ptr = unsafe.getLong(mem);
        slabId = getSlabIdFromPtr(ptr);
        // Unlock slab and allow compaction
        readUnlockSlab(slabId);
      }
    }
    } finally{
      lock.readLock().unlock();
    }
  }
	
	/**
	 * Read lock slab.
	 *
	 * @param slabId the slab id
	 */
	private void readLockSlab(int slabId){
		lockArray[slabId].readLock().lock();
	}
	
	/**
	 * Write unlock slab.
	 *
	 * @param slabId the slab id
	 */
	@SuppressWarnings("unused")
  private void writeUnlockSlab(int slabId){
		lockArray[slabId].writeLock().unlock();
	}

	 /**
   * Write lock slab.
   *
   * @param slabId the slab id
   */
  @SuppressWarnings("unused")
  private void writeLockSlab(int slabId){
    lockArray[slabId].writeLock().lock();
  }
  
  /**
   * Read unlock slab.
   *
   * @param slabId the slab id
   */
  private void readUnlockSlab(int slabId){
    lockArray[slabId].readLock().unlock();
  }
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#isLockEnabled()
	 */
	@Override
	public final boolean isLockEnabled() {		
		return lockEnabled;
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#setLockEnabled(boolean)
	 */
	@Override
	public void setLockEnabled(boolean b) {
		this.lockEnabled = b;
		
	}

	@Override
	public long realAddress(long mem) {
		long ptr = unsafe.getLong(mem);
		// Returns real address
		return (ptr + PTR_SIZE) & 0xffffffffffffL;
	}

	@Override
	public boolean isCompactionActive() {		
		if(canControlCompaction()){
			return compactionEnabled;
		} else{
			return maxMemory <= memoryAllocatedExact();
		}
	}

	@Override
	public boolean isCompactionSupported() {
		return true;
	}

	/**
	 * We can control compaction only when 'maxMemory=0; - is not set
	 * If max memory is set we ignore
	 */
	@Override
	public void setCompactionActive(boolean b) {
		if( canControlCompaction()){
			compactionEnabled = b;
		}		
	}
	
	private final boolean canControlCompaction()
	{
		return maxMemory == 0;
	}

  @Override
  public void setSlabSizes(long[] sizes) {
    
    // We expect array of size 2 * totalSlabs
    if(sizes.length != 2 * totalSlabs) return;
    
    slabSizes = Arrays.copyOf(sizes, sizes.length);
    minSize = (int)slabSizes[0];
    for (int i = 0; i < slabSizes.length; i++) {
      if(slabSizes[i] <= maxBlockSize && i < totalSlabs){
        maxCommonBlockSlabId = i;
      }
    }
  }

  @Override
  public void setParetAllocator(Malloc parent) {
    this.parent = parent;    
  }
}
