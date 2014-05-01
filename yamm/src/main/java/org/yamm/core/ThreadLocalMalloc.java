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
 * The Class ThreadLocalMalloc.
 */
public class ThreadLocalMalloc implements Malloc {

	
	/** The factory. */
	final MallocFactory factory ;
	
	/** The lock enabled. */
	volatile boolean lockEnabled;
	
	// Thread local does not support memory limit  yet.
	@SuppressWarnings("unused")
  private long maxMemory;
	
	/** The tls malloc. */
	private ThreadLocal<Malloc> tlsMalloc = new ThreadLocal<Malloc>(){
		@Override
		protected Malloc initialValue() {
			// TODO Auto-generated method stub
			return factory.newMalloc();
		}
		
	};
	
	/**
	 * Instantiates a new thread local malloc.
	 *
	 * @param factory the factory
	 */
	public ThreadLocalMalloc(MallocFactory factory){
		this.factory = factory;
	}
	
	/**
	 * Gets the.
	 *
	 * @return the malloc
	 */
	public Malloc get(){
		return tlsMalloc.get();
	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#free(long)
	 */
	@Override
	public void free(long ptr) {
		Malloc malloc = tlsMalloc.get();
		malloc.setLockEnabled(lockEnabled);
		malloc.free(ptr);
	}


	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#malloc(long)
	 */
	@Override
	public long malloc(long size) {
		//System.out.println("m");
		Malloc malloc = tlsMalloc.get();
		malloc.setLockEnabled(lockEnabled);
		return malloc.malloc(size);
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#mallocUsableSize(long)
	 */
	@Override
	public final long mallocUsableSize(long ptr) {
		Malloc malloc = tlsMalloc.get();
		malloc.setLockEnabled(lockEnabled);
		return malloc.mallocUsableSize(ptr);
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#memoryAllocated()
	 */
	@Override
	public long memoryAllocated() {
		//TODO - global memory allocated
		Malloc malloc = tlsMalloc.get();
		malloc.setLockEnabled(lockEnabled);
		return malloc.memoryAllocated();
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#ralloc(long, long)
	 */
	@Override
	public long ralloc(long ptr, long newSize) {
		Malloc malloc = tlsMalloc.get();
		malloc.setLockEnabled(lockEnabled);
		return malloc.ralloc(ptr, newSize);
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#setLockEnabled(boolean)
	 */
	@Override
	public void setLockEnabled(boolean b) {
		this.lockEnabled = b;

	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#isLockEnabled()
	 */
	@Override
	public boolean isLockEnabled() {
		// TODO Auto-generated method stub
		return false;
	}
	
	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#lockAddress(long)
	 */
	@Override
	public final long lockAddress(long ptr) {
		Malloc malloc = tlsMalloc.get();
		malloc.setLockEnabled(lockEnabled);
		return malloc.lockAddress(ptr);
	}

	/* (non-Javadoc)
	 * @see org.yamm.core.Malloc#unlockAddress(long)
	 */
	@Override
	public final void unlockAddress(long ptr) {
		Malloc malloc = tlsMalloc.get();
		malloc.setLockEnabled(lockEnabled);
		malloc.unlockAddress(ptr);

	}

	@Override
	public long realAddress(long ptr) {
		// TODO: This can be optimized for NoCompaction malloc
		Malloc malloc = tlsMalloc.get();
		return malloc.realAddress(ptr);
	}

	@Override
	public long getMaxMemorySize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setMaxMemorySize(long size) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isCompactionActive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCompactionSupported() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setCompactionActive(boolean b) {
		// TODO Auto-generated method stub
		
	}

  @Override
  public long[] getSlabSizes() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setSlabSizes(long[] sizes) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setParetAllocator(Malloc parent) {
    // TODO Auto-generated method stub
    
  }

}
