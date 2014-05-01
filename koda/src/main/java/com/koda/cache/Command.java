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
import java.io.Serializable;
import java.nio.ByteBuffer;

import com.koda.NativeMemoryException;

// TODO: Auto-generated Javadoc
/**
 * The Interface Code.
 *
 * @param <T> the generic type
 */
public interface Command<T> extends LockStripeAware, Serializable {
	

	/**
	 * Execute.
	 *
	 * @param ptr the ptr
	 * @param cache the cache
	 * @return true, if successful
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean execute(long ptr, OffHeapCache cache) throws NativeMemoryException, IOException;
	

	/**
	 * Execute command (operation) on cache, using ByteBuffer as key.
	 *
	 * @param key the key
	 * @param cache the cache
	 * @return true, if successful
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean execute(ByteBuffer key, OffHeapCache cache) throws NativeMemoryException, IOException;
	
	
	/**
	 * Gets the result.
	 *
	 * @return the result
	 */
	public T getResult();

}
