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
package com.koda.io.serde;

import java.io.IOException;
import java.nio.ByteBuffer;

// TODO: Auto-generated Javadoc
/**
 * The Interface Writable.
 *
 * @param <T> the generic type
 */
public interface Writable<T> {

	/**
	 * Write object.
	 *
	 * @param buf the buf
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void write(ByteBuffer buf) throws IOException;
	
	/**
	 * Read object.
	 *
	 * @param buf the buf
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void read(ByteBuffer buf) throws IOException;
	
	/**
	 * Read.
	 *
	 * @param buf the buf
	 * @param obj the obj
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void read(ByteBuffer buf, T obj) throws IOException;
	
	/**
	 * Gets the iD.
	 *
	 * @return the iD
	 */
	public int getID();
	
}
