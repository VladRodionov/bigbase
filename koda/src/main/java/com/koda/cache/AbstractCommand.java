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
package com.koda.cache;


import java.io.IOException;
import java.nio.ByteBuffer;

import com.koda.NativeMemoryException;

// TODO: Auto-generated Javadoc
/**
 * The Class AbstractCodeImpl.
 *
 * @param <T> the generic type
 */
@SuppressWarnings("serial")
public class AbstractCommand<T> implements Command<T> {

	/** The m result. */
	protected T mResult; 
	
	/** The m lock stripe number. */
	protected int mLockStripeNumber;
	
	
	/**
	 * Instantiates a new abstract code impl.
	 */
	public AbstractCommand(){}
	




	/* (non-Javadoc)
	 * @see com.koda.cache.Code#execute(long, com.koda.cache.OffHeapCache)
	 */
	@Override
	public boolean execute(long ptr, OffHeapCache cache) throws NativeMemoryException, IOException {
		return false;

	}

	/* (non-Javadoc)
	 * @see com.koda.cache.Code#getResult()
	 */
	@Override
	public T getResult() {
		
		return mResult;
	}



	/* (non-Javadoc)
	 * @see com.koda.cache.Command#execute(java.nio.ByteBuffer, com.koda.cache.OffHeapCache)
	 */
	@Override
	public boolean execute(ByteBuffer key, OffHeapCache cache)
			throws NativeMemoryException, IOException {
		// TODO Auto-generated method stub
		return false;
	}



	/* (non-Javadoc)
	 * @see com.koda.cache.LockStripeAware#getLockStripe()
	 */
	@Override
	public int getLockStripe() {
		
		return mLockStripeNumber;
	}



	/* (non-Javadoc)
	 * @see com.koda.cache.LockStripeAware#setLockStripe(int)
	 */
	@Override
	public void setLockStripe(int stripe) {
		mLockStripeNumber = stripe;		
	}


	
}
