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
package com.koda.cache.eviction;

// TODO: Auto-generated Javadoc
/**
 * The listener interface for receiving eviction events.
 * The class that is interested in processing a eviction
 * event implements this interface, and the object created
 * with that class is registered with a component using the
 * component's <code>addEvictionListener<code> method. When
 * the eviction event occurs, that object's appropriate
 * method is invoked.
 *
 * @see EvictionEvent
 */
public interface EvictionListener {
	
	/**
	 * The Enum Reason.
	 */
	public static enum Reason{ 
		/** The ALGO. */
			ALGO, 
		/** The EXPIRED. */
			EXPIRED
	};
	
	/**
	 * Evicted.
	 *
	 * @param ptr the ptr
	 * @param reason the reason
	 * @param nanoTime the nano time
	 */
	public void evicted(long ptr, Reason reason, long nanoTime);
}
