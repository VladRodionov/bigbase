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

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

// TODO: Auto-generated Javadoc
/**
 * The Class Hints.
 */
public class Hints {

	/** The Constant CACHE_BYPASS. */
	public final static byte[] CACHE_BYPASS     = "cache$bypass".getBytes();
	
	/** The Constant CACHE_EXACT. */
	public final static byte[] CACHE_EXACT      = "cache$exact".getBytes();
	
	/** The Constant CACHE_ON_WRITE. */
	public final static byte[] CACHE_ON_WRITE   = "cache$on-write".getBytes();
	
	
	/**
	 * Bypass cache.
	 *
	 * @param get the get
	 * @return true, if successful
	 */
	public final static boolean bypassCache(Get get)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		return map.containsKey(CACHE_BYPASS);
	}
	
	/**
	 * Bypass cache.
	 *
	 * @param get the get
	 * @param clean the clean
	 * @return true, if successful
	 */
	public final static boolean bypassCache(Get get, boolean clean)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		boolean result = map.containsKey(CACHE_BYPASS);
		if(result && clean){
			map.remove(CACHE_BYPASS);
		}
		return result;
	}
	
	/**
	 * Exact cache.
	 *
	 * @param get the get
	 * @return true, if successful
	 */
	public final static boolean exactCache(Get get)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		return map.containsKey(CACHE_EXACT);
	}
	
	/**
	 * Exact cache.
	 *
	 * @param get the get
	 * @param clean the clean
	 * @return true, if successful
	 */
	public final static boolean exactCache(Get get, boolean clean)
	{
		Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
		boolean result = map.containsKey(CACHE_EXACT);
		if(result && clean){
			map.remove(CACHE_EXACT);
		}
		return result;
	}
	/**
	 * Currently we do not support hints in Increment
	 * Only Put and Append.
	 *
	 * @param op the op
	 * @return true, if successful
	 */
	public final static boolean onWriteCache( Mutation op)
	{
		if( op instanceof Put){
			Put put = (Put) op;
		    Map<byte[], List<KeyValue>> map = put.getFamilyMap();
		    return map.containsKey(CACHE_ON_WRITE);
		} else if( op instanceof Append){
			Append append = (Append) op;
			Map<byte[], List<KeyValue>> map = append.getFamilyMap();
		    return map.containsKey(CACHE_ON_WRITE);
			
		}
		return false;
	}
	
	/**
	 * On write cache.
	 *
	 * @param op the op
	 * @param clean the clean
	 * @return true, if successful
	 */
	public final static boolean onWriteCache( Mutation op, boolean clean)
	{
		boolean result = false;
		Map<byte[], List<KeyValue>> map = null;
		if( op instanceof Put){
			Put put = (Put) op;
		    map = put.getFamilyMap();
		} else if( op instanceof Append){
			Append append = (Append) op;
			map = append.getFamilyMap();			
		}
	    if( map != null){
	    	result = map.containsKey(CACHE_ON_WRITE);
	    	if(result && clean){
	    		map.remove(CACHE_ON_WRITE);
	    	}
	    }	    
		return result;
	}
}
