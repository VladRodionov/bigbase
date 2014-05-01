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
 * The Class ReadWriteLock.
 *
 * @author vrodionov
 */
public class ReadWriteLock {

	/**
	 * The Enum Mode.
	 */
	public static enum Mode{
		
		/** The READ. */
		READ, 
 /** The WRITE. */
 WRITE
	}
	
	/** The Constant WRITE_LOCK. */
	private final static int WRITE_LOCK = -1;
	
	/** The Constant NO_LOCK. */
	private final static int NO_LOCK = 0;
	
	/** The lock. */
	private AtomicInteger lock = new AtomicInteger(NO_LOCK);
	
	/**
	 * Instantiates a new read write lock.
	 */
	public ReadWriteLock()
	{
		
	}
	
	/**
	 * Lock.
	 *
	 * @param mode the mode
	 */
	public void lock(Mode mode)
	{
		if(mode == Mode.WRITE){
			while(!lock.compareAndSet(NO_LOCK, WRITE_LOCK))/*Busy spin lock*/;
			return;
		} else{// Mode.READ
			int expect = NO_LOCK;
			
			while(!lock.compareAndSet(expect, expect+1)){
				// Busy spin lock
				int val = lock.get();
				if(val == WRITE_LOCK) {
					expect = NO_LOCK;
				} else{
					expect = val;
				}
			}
		}
	}
	
	/**
	 * Unlock.
	 *
	 * @param mode the mode
	 */
	public void unlock(Mode mode)
	{
		if(mode == Mode.WRITE){
			lock.set(NO_LOCK);
		} else{
			lock.decrementAndGet();
		}
	}
	
	
}
