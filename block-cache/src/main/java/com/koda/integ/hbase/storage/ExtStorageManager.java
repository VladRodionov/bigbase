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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.koda.cache.OffHeapCache;
import com.koda.integ.hbase.blockcache.OffHeapBlockCache;


// TODO: Auto-generated Javadoc
/**
 * The Class ExtStorageManager.
 */
public class ExtStorageManager {
	  /** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(ExtStorageManager.class);
	
	/** The instance. */
	private static ExtStorageManager instance;
	
	/**
	 * Gets the single instance of ExtStorageManager.
	 *
	 * @return single instance of ExtStorageManager
	 */
	public static ExtStorageManager getInstance()
	{
		synchronized(ExtStorageManager.class){
			if(instance == null){
				instance = new ExtStorageManager();
			}
			return instance;
		}
	}
	
	
	/**
	 * Gets the storage.
	 *
	 * @param config the config
	 * @param cache the cache
	 * @return the storage
	 */
	public ExtStorage getStorage(Configuration config , OffHeapCache cache)
	{
		String implClass = config.get(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_IMPL, "com.koda.integ.hbase.storage.FileExtStorage");
		
		try {
			Class<?> cls = Class.forName(implClass);
			ExtStorage storage =  (ExtStorage) cls.newInstance();
			storage.config(config, cache);
			return storage;
		} catch (Exception e) {
			LOG.fatal(implClass, e);
		}
		return null;
	}
	
}
