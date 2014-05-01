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

import java.util.concurrent.locks.ReentrantReadWriteLock;


// TODO: Auto-generated Javadoc
/**
 * This class inherits all semantics of.
 *
 * @author vrodionov
 */
public class SpinReadWriteLock extends ReentrantReadWriteLock {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -4752822722219648140L;

	/** The m lock. */
	private SpinLock mLock = new SpinLock();
	
	/** The m stripe index. */
	private int mStripeIndex;
	
	/**
	 * Instantiates a new spin read write lock.
	 */
	public SpinReadWriteLock()
	{
		super();
	}
	
	/**
	 * Instantiates a new spin read write lock.
	 *
	 * @param fair the fair
	 */
	public SpinReadWriteLock(boolean fair)
	{
		super(fair);
	}
	
	
	/**
	 * Sets the stripe index.
	 *
	 * @param index the new stripe index
	 */
	public void setStripeIndex(int index)
	{
		this.mStripeIndex = index;
	}
	
	/**
	 * Gets the stripe index.
	 *
	 * @return the stripe index
	 */
	public int getStripeIndex()
	{
		return mStripeIndex;
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
