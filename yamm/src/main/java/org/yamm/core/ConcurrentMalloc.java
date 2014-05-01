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

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


// TODO: Auto-generated Javadoc
/**
 * The Class ThreadLocalMalloc.
 */
public class ConcurrentMalloc implements Malloc {

	
	/** The factory. */
	final MallocFactory factory ;
	
	/** The concurrency level. Max is 128 */
	final int concurrencyLevel ;
	/** The lock enabled. */
	//volatile boolean lockEnabled;
	
	/** Array of allocators. */
	Malloc[] allocators;
	
	/** Locks. */
	ReentrantLock[] locks;
	
	/** The rnd. */
	Random rnd = new Random(); 
	
	private long maxMemory;
	/**
	 * Instantiates a new thread local malloc.
	 *
	 * @param factory the factory
	 */
	public ConcurrentMalloc(MallocFactory factory){
		this.factory = factory;
		this.concurrencyLevel = Runtime.getRuntime().availableProcessors()/2;
		init();
	}

	/**
	 * Instantiates a new thread local malloc.
	 *
	 * @param factory the factory
	 * @param concurrencyLevel the concurrency level
	 */
	public ConcurrentMalloc(MallocFactory factory, int concurrencyLevel){
		this.factory = factory;
		this.concurrencyLevel = concurrencyLevel;
		init();
	}
	
	
	public ConcurrentMalloc(MallocFactory factory, int concurrencyLevel, long maxMemory){
		this.factory = factory;
		this.concurrencyLevel = concurrencyLevel;
		this.maxMemory = maxMemory;
		init();
	}
	
	/**
	 * Inits the.
	 */
	private void init() {
		allocators = new Malloc[concurrencyLevel];
		locks = new ReentrantLock[concurrencyLevel];
		for(int i=0; i < concurrencyLevel; i++){
			allocators[i] = factory.newMalloc();
			locks[i] = new ReentrantLock();
		}		
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#free(long)
	 */
	@Override
	public void free(long ptr) {
		int id = getAllocatorIdFor(ptr);
		// Lock
		Lock lock = locks[id];
		try{
			lock.lock();		
			Malloc malloc = allocators[id];
			//malloc.setLockEnabled(lockEnabled);
			ptr = getPtr(ptr);
			malloc.free(ptr);
		} finally{
			lock.unlock();
		}
	}


	/**
	 * Gets the ptr.
	 *
	 * @param ptr the ptr
	 * @return the ptr
	 */
	private final long getPtr(long ptr) {
		return ptr & 0xffffffffffffffL;
	}

	/**
	 * Gets the allocator id for.
	 *
	 * @param ptr the ptr
	 * @return the allocator id for
	 */
	private int getAllocatorIdFor(long ptr) {
		return (int) ((ptr >>> 56) & 0x7f) - 1;
	}

	/**
	 * Alloc id to ptr.
	 *
	 * @param ptr the ptr
	 * @param id the id
	 * @return the long
	 */
	private long allocIdToPtr(long ptr , int id)
	{
		if(ptr == 0L) return 0L;
	  long ret = ((long) id + 1) << 56 | ptr;
		return ret;
	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#malloc(long)
	 */
	@Override
	public long malloc(long size) {
		
		int id = rnd.nextInt(concurrencyLevel);
		Lock lock = null;
		// Select first available allocator
		while( true){
			lock = locks[id];
			if(lock.tryLock()){
				break;
			} else{
				id = (id +1) % concurrencyLevel;
			}
		}		
		
		int count =0;
		long ptr = 0L;
		while( ((ptr = mallocFromAllocator(size, id, lock)) == 0L) && count++ < concurrencyLevel){
		  id = (id +1) % concurrencyLevel;
		  lock = locks[id];
		  // get lock
		  lock.lock();
		}
		
		return ptr;

	}

	private long mallocFromAllocator(long size, int id, Lock lock)
	{
	   boolean isCompactionActive = isCompactionActive();
	    boolean isCompactionSupported = isCompactionSupported();

	    try{
	      if( isCompactionActive == true && isCompactionSupported == false){
	        return 0L;// can't allocate
	      }
	      Malloc malloc = allocators[id];
	      //malloc.setLockEnabled(lockEnabled);
	      malloc.setCompactionActive(isCompactionActive);
	      long ptr = malloc.malloc(size);
	      // We fail fast? We do not try other allocators
	      if(ptr == 0) return 0L;
	      return allocIdToPtr(ptr, id);
	    } finally{
	      lock.unlock();
	    }
	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#mallocUsableSize(long)
	 */
	@Override
	public final long mallocUsableSize(long ptr) {
		// Get first allocator
	  
		Malloc malloc = allocators[0];
		return malloc.mallocUsableSize(getPtr(ptr));
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#memoryAllocated()
	 */
	@Override
	public long memoryAllocated() {
		// Returns global memory
		long allocd = 0;
		for(Malloc m: allocators){
			final long alloc = m.memoryAllocated();
		  allocd += alloc;			
		}
		return allocd;
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#ralloc(long, long)
	 */
	@Override
	public long ralloc(long ptr, long newSize) {
		int id = getAllocatorIdFor(ptr);
		// Lock
		Lock lock = locks[id];
		try{
			lock.lock();		
			Malloc malloc = allocators[id];
			malloc.setCompactionActive(isCompactionActive());
			//malloc.setLockEnabled(lockEnabled);
			ptr = getPtr(ptr);
			
			return allocIdToPtr(malloc.ralloc(ptr, newSize), id);
		} finally{
			lock.unlock();
		}
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#setLockEnabled(boolean)
	 */
	@Override
	public void setLockEnabled(boolean b) {
		//this.lockEnabled = b;

	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#isLockEnabled()
	 */
	@Override
	public boolean isLockEnabled() {
		// TODO Auto-generated method stub
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#lockAddress(long)
	 */
	@Override
	public final long lockAddress(long ptr) {
		int id = getAllocatorIdFor(ptr);
		Malloc malloc = allocators[id];
		//malloc.setLockEnabled(lockEnabled);
		long mem = getPtr(ptr);
		if(mem == 0){
			//System.out.println("Mem=0 ptr="+Long.toString(ptr, 16));
		  throw new RuntimeException("Attempt to lock memory address 0");
		}
		return malloc.lockAddress(mem);
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#unlockAddress(long)
	 */
	@Override
	public final void unlockAddress(long ptr) {
		int id = getAllocatorIdFor(ptr);
		Malloc malloc = allocators[id];
		//malloc.setLockEnabled(lockEnabled);
		malloc.unlockAddress(getPtr(ptr));

	}

	@Override
	public long realAddress(long ptr) {
		// Short circuit for NoCompaction
		int id = getAllocatorIdFor(ptr);
		Malloc malloc = allocators[id];
		long mem = getPtr(ptr);
		if(mem == 0){
			//System.out.println("Mem=0 ptr="+Long.toString(ptr, 16));
		  throw new RuntimeException("Attempt to dereference zero pointer");
		}
		return malloc.realAddress(mem);
	}

	@Override
	public long getMaxMemorySize() {
		
		return maxMemory;
	}

	@Override
	public void setMaxMemorySize(long size) {
		this.maxMemory = size;	
		// For all allocators
		// set max as well
		long allocMax = size / concurrencyLevel;
		System.out.println("[concurrent-malloc] global max set to "+size+" slab allocators max set to "+allocMax);
		for(Malloc m: allocators){
		  m.setMaxMemorySize(allocMax);
		}
		
	}

	@Override
	public final boolean isCompactionActive() {		
		return maxMemory > 0 && maxMemory <= memoryAllocated();
	}

	@Override
	public boolean isCompactionSupported() {
		
		return allocators[0].isCompactionSupported();
	}

	@Override
	public void setCompactionActive(boolean b) {
		// We ignore this method		
	}
	
	public int getConcurrencyLevel(){
	  return concurrencyLevel;
	}

  @Override
  public long[] getSlabSizes() {
    
    return allocators[0].getSlabSizes();
  }

  @Override
  public void setSlabSizes(long[] sizes) {
    // This method MUST be called before first memory allocation happens
    for(Malloc m : allocators){
      m.setSlabSizes(sizes);
    }    
  }

  @Override
  public void setParetAllocator(Malloc parent) {
    // TODO Auto-generated method stub
    
  }

}
