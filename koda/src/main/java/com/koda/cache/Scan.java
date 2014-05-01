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

import com.koda.util.Configuration;

// TODO: Auto-generated Javadoc
/**
 * The Class Scan.
 *
 * @param <T> the generic type
 */
public interface Scan<T> {

	
	
	/**
	 * Configure.
	 *
	 * @param cfg the cfg
	 */
	public void configure(Configuration cfg);
	/**
	 * Scan.
	 *
	 * @param ptr the ptr
	 */
	public void scan (long ptr);
	
	/**
	 * Finalize.
	 */
	public void finish();
	
	/**
	 * Gets the result.
	 *
	 * @return the result
	 */
	public T getResult();
		
	
}
