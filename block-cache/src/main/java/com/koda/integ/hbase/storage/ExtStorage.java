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
package com.koda.integ.hbase.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.koda.cache.OffHeapCache;


// TODO: Auto-generated Javadoc
/**
 * The Interface ExtStorage.
 */
public interface ExtStorage {

	/**
	 * Config.
	 *
	 * @param config the config
	 * @param cache the cache
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void config(Configuration config, OffHeapCache cache) throws IOException;
	
	/**
	 * Store data. Its data format agnostic - it just stores BLOBs
	 * and return storage opaque handle which is used to access this data
	 * 0..3 - blob size
	 * 4 .. - blob 
	 *
	 * @param buf the buf
	 * @return the storage handle
	 */
	public StorageHandle storeData(ByteBuffer buf);
	
	/**
	 * Store multiple objects
	 * 0..3  - total size of a batch in bytes
	 * 4 ..  - blocks
	 * Each block:
	 * 0..3  - size
	 * 4 ..  - block data (blob)
	 *
	 * @param buf the buf
	 * @return the array of storage handles
	 */
	public List<StorageHandle> storeDataBatch(ByteBuffer buf);
	
	/**
	 * Gets the data into byte buffer by a given handle.
	 *
	 * @param storeHandle the store handle
	 * @param buf the buf
	 * @return the data
	 */
	public StorageHandle getData(StorageHandle storeHandle, ByteBuffer buf);
	
	/**
	 * Close storage. Release all resources.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void close() throws IOException;
	
	/**
	 * Flush the storage.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void flush() throws IOException;
	
	public long size();
	
	public void shutdown(boolean isPersistent) throws IOException;
	
	public long getMaxStorageSize();
	
	public StorageHandle newStorageHandle();
	
	public boolean isValid(StorageHandle h);
					
	
}
