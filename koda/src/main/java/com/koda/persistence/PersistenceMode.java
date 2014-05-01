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

// TODO: Auto-generated Javadoc
/**
 * The Enum PersistenceType.
 * Not all persistence modes can be supported 
 * by a particulal implementaion of DataStore.
 *  
 */
public enum PersistenceMode {

	/** The NONE. */
	NONE(0),
	/** 
	 *  ONDEMAND
	 * Cache is loaded on start up and saved on shutdown (only).
	 * This is no overflow mode (disk cache is 1-1 RAM cache) RAM size limited
	 * How to handle expiration time and last_time_used/ total_times_used fields
	 * 1. Add those fields to key
	 * 2. Add thos fields to value	 
	 * 
	 * No fault tolerance.
	 */
	
	ONDEMAND(1),
	// Snapshots are made periodically
	/** 
	 * The SNAPSHOT mode (not atomic). 
	 * Cache is saved to disk periodically (in background). Must fit RAM. No overflow support 
	 * Moderate fault tolerance (we can restore from last snapshot)
	 * 
	 * */
	
	SNAPSHOT(2),
	// On-line write behind
	/** 
	 * The WRITE behind store 
	 * Batch updates. Batch size is configurable.
	 * Batch update max interval is configurable.
	 * Sync on batch is configuarble (by default its true)
	 * Overflow to disk = true;
	 * Cache size >= RAM size
	 * Good fault tolerance (we can loose only several seconds of updates if application craches)
	 * The max batch interval as well as batch size are configurable parameters
	 * 
	 * */
	WRITE_BEHIND(3),
	// On-line write through (slow)
	/** The WRITE_THROUGH mode
	 * No batch updates. Sync writes to disk
	 * Overflow to disk = true;
	 * Cache size >= RAM size
	 * Excelelnt fault tolerance . 
	 * Poor performance.
	 * 
	 * */
	WRITE_THROUGH(4),
	
	/** In WRITE_AROUND mode we write data directly to store, bypassing cache. */
	WRITE_AROUND(5);
	
	/** The id. */
	private int id;
	
	/**
	 * Instantiates a new persistence type.
	 *
	 * @param id the id
	 */
	private PersistenceMode(int id)
	{
		this.id = id;
	}
	
	/**
	 * Id.
	 *
	 * @return the int
	 */
	public int id() {return id;}
}
