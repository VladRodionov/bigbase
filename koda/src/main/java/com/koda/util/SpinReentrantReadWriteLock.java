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
package com.koda.util;

import java.util.concurrent.atomic.AtomicInteger;



// TODO: Auto-generated Javadoc
/**
 * N readers - 1 writer lock. Lock is Reentrant for writes and conditionally reentrant
 * on reads
 *
 * @author vrodionov
 */
public class SpinReentrantReadWriteLock {

	/** The m lock. */
	private SpinLock mLock = new SpinLock();
	
	/** The m readers. */
	private int mReaders;
	
	/** The m counter. */
	private AtomicInteger mCounter = new AtomicInteger(0);
	
	/** The BACKOF f_ min. */
	private long BACKOFF_MIN= 20;
	
	/** The BACKOF f_ max. */
	private long BACKOFF_MAX = 20000;
	
	/** The BACKOF f_ int. */
	private long BACKOFF_INT = 20;
	
	/** The BACKOF f_ mi n_ sleep. */
	private long BACKOFF_MIN_SLEEP = 1000; // ns - 1microsec
	
	/** The BACKOF f_ ma x_ sleep. */
	private long BACKOFF_MAX_SLEEP = 1000000;// ns - 1 millisec
	
	/** The BACKOF f_ in t_ sleep. */
	private long BACKOFF_INT_SLEEP = 1000;
		
	/** The m current writer. */
	ThreadLocal<Thread> mCurrentWriter = new ThreadLocal<Thread>();
	
	/**
	 * Instantiates a new spin read write lock.
	 *
	 * @param readers the readers
	 */
	public SpinReentrantReadWriteLock(int readers)
	{
		this.mReaders = readers;
	}
	

	/**
	 * Lock read.
	 *
	 * @return true, if successful
	 */
	public boolean readLock()
	{
		int i =0;
		long count = BACKOFF_MIN;
		long timeout = BACKOFF_MIN_SLEEP;
		if(mCurrentWriter.get() == Thread.currentThread()){
			// get read lock if owns write
			return true;
		}
		while(!mCounter.compareAndSet(i, i+1)){
			
			i++;
			
			if(i == mReaders){
				i =0;
				// spin a little bit
				if(count < BACKOFF_MAX){
					int counter =0;
					while(counter++ < count);
					count+= BACKOFF_INT;
				} else{
					count = BACKOFF_MIN;

					if(timeout > BACKOFF_MAX_SLEEP){
						return false;
					} else{
						long millis = timeout/1000000;
						long nanos = timeout - millis*1000000;
						try {						
							Thread.sleep(millis, (int)nanos);
						} catch (InterruptedException e) {

						}
						timeout+= BACKOFF_INT_SLEEP;
					}
				}
			}
		}
		return true;
	}
	
	
	/**
	 * Read unlock.
	 */
	public void readUnlock()
	{
		if(mCurrentWriter.get() != Thread.currentThread()){
			mCounter.decrementAndGet();
		}
	}
	
	
	/**
	 * Write lock.
	 *
	 * @return true, if successful
	 */
	public boolean writeLock()
	{
		if(mCurrentWriter.get() == Thread.currentThread()){
			mCounter.decrementAndGet();
			return true;
		}
		
		long count = BACKOFF_MIN;
		long timeout = BACKOFF_MIN_SLEEP;
		while(!mCounter.compareAndSet(0, -1) ){

			if(count < BACKOFF_MAX){
				int counter =0;
				while(counter++ < count);
				count+= BACKOFF_INT;
			} else{
				count = BACKOFF_MIN;

				if(timeout > BACKOFF_MAX_SLEEP){
					return false;
				} else{
					long millis = timeout/1000000;
					long nanos = timeout - millis*1000000;
					try {						
						Thread.sleep(millis, (int)nanos);
					} catch (InterruptedException e) {

					}
					timeout+= BACKOFF_INT_SLEEP;
				}
			}
			
		}
		// sets current thread as a writer
		mCurrentWriter.set(Thread.currentThread());
		
		return true;
	}
	
	/**
	 * Write unlock.
	 */
	public void writeUnlock()
	{
		if(mCounter.get() == -1){
			mCurrentWriter.set(null);
		}
		mCounter.incrementAndGet();
	}
	
	/**
	 * Gets the spin lock.
	 *
	 * @return the spin lock
	 */
	public SpinLock getSpinLock()
	{
		return mLock;
	}
}
