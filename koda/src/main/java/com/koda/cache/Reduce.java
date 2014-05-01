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

import java.io.Serializable;
import java.util.List;

import com.koda.util.Configuration;

// TODO: Auto-generated Javadoc
/**
 * The Interface Reducer.
 *
 * @param <T> the generic type
 */
public interface Reduce<T> extends Serializable{

	
	
	/**
	 * Configure.
	 *
	 * @param cfg the cfg
	 */
	public void configure(Configuration cfg);
	/**
	 * Reduce. Reduces result of scan/query operation
	 *
	 * @param results the results
	 * @return the object
	 */
	public T reduce(List<T> results);
}
