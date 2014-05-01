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

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.config.CacheConfiguration;
import com.koda.config.ExtCacheConfiguration;
import com.koda.persistence.DiskStore;
import com.koda.persistence.ProgressListener;
import com.koda.persistence.rawfs.RawFSStore;


// TODO: Auto-generated Javadoc
/**
 * The Class CacheManager.
 */
public class CacheManager {
	
	
	/** The m caches. */
	private ConcurrentHashMap<String, OffHeapCache> mCaches = 
		new ConcurrentHashMap<String, OffHeapCache>();
	
	/** The s instance. */
	private static CacheManager sInstance;
	
	/**
	 * Gets the single instance of CacheManager.
	 *
	 * @return single instance of CacheManager
	 */
	public static CacheManager getInstance()
	{
		synchronized(CacheManager.class)
		{
			if(sInstance == null){
				sInstance = new CacheManager();
			}
			return sInstance;
		}
	}
	
	/**
	 * Creates the cache instance.
	 *
	 * @param config the cache config object
	 * @return the off heap cache
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 */
	public OffHeapCache createCache(CacheConfiguration config) throws KodaException
	{
		if( config == null) throw new IllegalArgumentException("Config is null");
		String fullName = getFullName(config);
		if(mCaches.contains(fullName)) throw new KodaException("cache already exists");
		OffHeapCache cache = null;
		synchronized(fullName.intern()){
			if(mCaches.contains(fullName)) throw new KodaException("cache already exists");
			try{
				cache = new OffHeapCache(config);
			}catch(Exception e){
				e.printStackTrace();
				throw new KodaException(e);
			}
			mCaches.putIfAbsent(fullName, cache);
		}

		return cache;
	}

	
	
	/**
	 * Creates the cache instance.
	 *
	 * @param config the cache config object
	 * @return the off heap cache
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 */
	public OffHeapCache getOrCreateCache(CacheConfiguration config) throws KodaException, NativeMemoryException
	{
		if( config == null) throw new IllegalArgumentException("Config is null");
		String fullName = getFullName(config);
		OffHeapCache cache= mCaches.get(fullName);
		if(cache == null) cache = createCache(config);
		return cache;
	}
	
	
	/**
	 * Gets the cache instance.
	 *
	 * @param config the cache config object
	 * @param pl the pl
	 * @return the off heap cache
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public OffHeapCache getCache(CacheConfiguration config, ProgressListener pl) throws KodaException, IOException
	{
		if( config == null) throw new IllegalArgumentException("Config is null");
		String fullName = getFullName(config);
		OffHeapCache cache= mCaches.get(fullName);
		DiskStore ds = null;
		if(cache == null) {
			// Try to load cache
			if(config.getDiskStoreConfiguration() != null){
				synchronized(fullName.intern()){
					ds = DiskStore.getDiskStore(config);
					if(ds != null){
						if( ds instanceof RawFSStore){
							cache = ((RawFSStore)ds).loadFast(pl);
						} else{
							cache = ds.load(pl);
						}
					} 
					if(cache != null){
						mCaches.putIfAbsent(fullName, cache);
					}
				}
			}
						
			if(cache == null){
				// OK create new one
				cache = createCache(config);
			}
		}
		if(ds != null) cache.setDiskStore(ds);
		return cache;
	}
	
	
	/**
	 * Creates the cache instance.
	 *
	 * @param config the cache config object
	 * @return the off heap cache
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 */
	public OffHeapCache createCache(ExtCacheConfiguration config) throws KodaException, NativeMemoryException
	{
		if( config == null) throw new IllegalArgumentException("Config is null");
		String fullName = getFullName(config.getCacheConfig());
		if(mCaches.contains(fullName)) throw new KodaException("cache already exists");
		OffHeapCache cache = null;
		synchronized(fullName.intern()){
			if(mCaches.contains(fullName)) throw new KodaException("cache already exists");
			try{
				cache = new OffHeapCache(config);
			}catch(Exception e){
				throw new KodaException(e);
			}
			mCaches.putIfAbsent(fullName, cache);
		}

		return cache;
	}

	
	
	/**
	 * Creates the cache instance.
	 *
	 * @param config the cache config object
	 * @return the off heap cache
	 * @throws KodaException the koda exception
	 * @throws NativeMemoryException the native memory exception
	 */
	public OffHeapCache getOrCreateCache(ExtCacheConfiguration config) throws KodaException, NativeMemoryException
	{
		if( config == null) throw new IllegalArgumentException("Config is null");
		String fullName = getFullName(config.getCacheConfig());
		OffHeapCache cache= mCaches.get(fullName);
		if(cache == null) cache = createCache(config);
		return cache;
	}
	/**
	 * Gets the.
	 *
	 * @param fullName the full name
	 * @return the off heap cache
	 */
	public OffHeapCache get(String fullName)
	{
		return mCaches.get(fullName);
	}
	
	/**
	 * Gets the full name.
	 *
	 * @param config the config
	 * @return the full name
	 */
	private String getFullName(CacheConfiguration config) {

		StringBuilder sb = new StringBuilder();
		sb.append(config.getCacheNamespace()).append(":").append(config.getCacheName());
		return sb.toString();
	}
	
	public void clearCaches(){
	  mCaches.clear();
	}
	
}
