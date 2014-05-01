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
package com.koda.persistence;

import java.io.IOException;

import com.koda.NativeMemoryException;

// TODO: Auto-generated Javadoc
/**
 * The Class DiskStoreException.
 */
public class DiskStoreException extends IOException {

	/**
	 * Instantiates a new disk store exception.
	 *
	 * @param e the e
	 */
//	public DiskStoreException(DBException e) {
//
//		super(e);
//	}

	/**
	 * Instantiates a new disk store exception.
	 *
	 * @param e the e
	 */
	public DiskStoreException(NativeMemoryException e) {
		super(e);
	}

	/**
	 * Instantiates a new disk store exception.
	 *
	 * @param e the e
	 */
	public DiskStoreException(Exception e) {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Instantiates a new disk store exception.
	 *
	 * @param string the string
	 */
	public DiskStoreException(String string) {
		// TODO Auto-generated constructor stub
	}

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = 1L;

}
