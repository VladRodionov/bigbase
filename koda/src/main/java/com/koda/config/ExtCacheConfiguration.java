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
package com.koda.config;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.koda.compression.Codec;
import com.koda.io.serde.SerDe;




// TODO: Auto-generated Javadoc
/**
 * This is class is used internally by CacheLoader.
 * Contains cache config and dynamic values
 * @author vrodionov
 *
 */
public class ExtCacheConfiguration  {
	/** The Constant LOG. */
	@SuppressWarnings("unused")
	private final static Logger LOG = Logger.getLogger(ExtCacheConfiguration.class);
	/** The cache config. */
	CacheConfiguration cacheConfig;
	
	/** The current allocated memory size. */
	long memorySize = 0;
	
	/** The total items. */
	long totalItems = 0;
	
	/** The epoch time. */
	long epochTime = 0;
	
	/** The is eviction active. */
	boolean isEvictionActive;
	
	/** The is disabled. */
	boolean isDisabled;
	
	
	/**
	 * Instantiates a new ext cache configuration.
	 */
	public ExtCacheConfiguration()
	{
		super();
	}


	/**
	 * Gets the cache config.
	 *
	 * @return the cache config
	 */
	public CacheConfiguration getCacheConfig() {
		return cacheConfig;
	}


	/**
	 * Sets the cache config.
	 *
	 * @param cacheConfig the new cache config
	 */
	public void setCacheConfig(CacheConfiguration cacheConfig) {
		this.cacheConfig = cacheConfig;
	}


	/**
	 * Gets the memory size.
	 *
	 * @return the memory size
	 */
	public long getMemorySize() {
		return memorySize;
	}


	/**
	 * Sets the memory size.
	 *
	 * @param memorySize the new memory size
	 */
	public void setMemorySize(long memorySize) {
		this.memorySize = memorySize;
	}


	/**
	 * Gets the total items.
	 *
	 * @return the total items
	 */
	public long getTotalItems() {
		return totalItems;
	}


	/**
	 * Sets the total items.
	 *
	 * @param totalItems the new total items
	 */
	public void setTotalItems(long totalItems) {
		this.totalItems = totalItems;
	}


	/**
	 * Gets the epoch time.
	 *
	 * @return the epoch time
	 */
	public long getEpochTime() {
		return epochTime;
	}


	/**
	 * Sets the epoch time.
	 *
	 * @param epochTime the new epoch time
	 */
	public void setEpochTime(long epochTime) {
		this.epochTime = epochTime;
	}


	/**
	 * Checks if is eviction active.
	 *
	 * @return true, if is eviction active
	 */
	public boolean isEvictionActive() {
		return isEvictionActive;
	}


	/**
	 * Sets the eviction active.
	 *
	 * @param isEvictionActive the new eviction active
	 */
	public void setEvictionActive(boolean isEvictionActive) {
		this.isEvictionActive = isEvictionActive;
	}


	/**
	 * Checks if is disabled.
	 *
	 * @return true, if is disabled
	 */
	public boolean isDisabled() {
		return isDisabled;
	}


	/**
	 * Sets the disabled.
	 *
	 * @param isDisabled the new disabled
	 */
	public void setDisabled(boolean isDisabled) {
		this.isDisabled = isDisabled;
	}
	
	/**
	 * Read.
	 *
	 * @param buf the buf
	 * @param codec the codec
	 * @return the ext cache configuration
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static ExtCacheConfiguration read(ByteBuffer buf, Codec codec) throws IOException
	{
		SerDe serde = SerDe.getInstance();
		return (ExtCacheConfiguration)serde.readCompressed(buf/*, codec*/);
	}
	
	/**
	 * Write.
	 *
	 * @param buf the buf
	 * @param cfg the cfg
	 * @param codec the codec
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void write(ByteBuffer buf, ExtCacheConfiguration cfg, Codec codec) throws IOException
	{
		SerDe serde = SerDe.getInstance();
		serde.writeCompressed(buf, cfg, codec);
	}
}
