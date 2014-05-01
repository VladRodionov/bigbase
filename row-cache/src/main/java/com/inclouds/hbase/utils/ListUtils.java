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
package com.inclouds.hbase.utils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class ListUtils.
 */
public class ListUtils {

	/**
	 * One list minus two list. Both lists MUST be sorted
	 *
	 * @param <T> the generic type
	 * @param one the one
	 * @param two the two
	 * @param comparator the comparator
	 * @param result the result
	 * @return the list
	 */
	public static <T> List<T> minus(List<T> one, List<T> two, Comparator<T> comparator, List<T> result)
	{
		for(int i=0; i < one.size(); i++){
			T obj = one.get(i);
			if(Collections.binarySearch(two, obj, comparator) >=0) {
				continue;
			}
			result.add(obj);
		}
		return result;
	}
	
	/**
	 * One list plus (union) two list. Both lists MUST be sorted
	 *
	 * @param <T> the generic type
	 * @param one the one
	 * @param two the two
	 * @param comparator the comparator
	 * @param result the result
	 * @return the list
	 */
	public static <T> List<T> plus(List<T> one, List<T> two, Comparator<T> comparator, List<T> result)
	{
		for(int i=0; i < one.size(); i++){
			T obj = one.get(i);
			int index = Collections.binarySearch(two, obj, comparator);
			if( index >= 0) {
				// Found. remove from second collection
				two.remove(index);
			}
			// Add object to result collection
			result.add(obj);
		}
		// Add rest of a second collection
		result.addAll(two);
		return result;
	}
}
