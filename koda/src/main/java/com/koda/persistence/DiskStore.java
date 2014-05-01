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
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.koda.cache.OffHeapCache;
import com.koda.config.CacheConfiguration;
import com.koda.config.DiskStoreConfiguration;


// TODO: Auto-generated Javadoc
/**
 * The Interface DiskStore.
 */
public abstract class DiskStore {
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(DiskStore.class);
	/**
	 * The Enum State.
	 */
	public static enum State{
		
		/** The DISABLED. */
		DISABLED /* store is disabled *//** The IDLE. */
 , 
		IDLE     /* in operational state ready *//** The LOAD. */
     , 
		LOAD     /* loading data from disk *//** The STORE. */
     , 
		STORE    /* storing data to disk *//** The SNAPSHOT. */
    ,  
		SNAPSHOT /* performing periodic snapshot */
	}
	
	/** The cancel requested. */
	protected boolean cancelRequested = false;
	
	/** The state. */
	protected AtomicReference<State> state = new AtomicReference<State>(State.IDLE);
	
	/** The store lock. */
	protected Object storeLock = new Object();
	
	/** The stores. */
	protected static ConcurrentHashMap<String, DiskStore> stores = 
		new ConcurrentHashMap<String, DiskStore>();
	
	
	/**
	 * Returns disk store for a given cache.
	 * The caller MUST call load(ProgressListener ) on start up
	 * to make sure all data was loaded
	 *
	 * @param cfg - cache configuration
	 * @return disk store instance
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	
	public synchronized static DiskStore getDiskStore(CacheConfiguration cfg)
		throws IOException
	{
		String key = cfg.getQualifiedName();
		DiskStore store = stores.get(key);
		if(store == null){
			Class<? extends DiskStore> clz = null;
			DiskStoreConfiguration diskCfg = cfg.getDiskStoreConfiguration();
			if(diskCfg == null){
				throw new IOException("disk store configuration is not defined");
			}
			clz = diskCfg.getDiskStoreImplementation();
			if(clz == null){
				throw new IOException("disk store implementation is not defined");
			}
			try {
				store = clz.newInstance();
			} catch (InstantiationException e) {
				throw new IOException(e);
			} catch (IllegalAccessException e) {
				throw new IOException(e);
			}
			
			store.init(cfg);
			stores.put(key, store);
		}
		return store;
	}
	


	/**
	 * Gets the state.
	 *
	 * @return the state
	 */
	public synchronized State getState(){
		return state.get();
	}
	
	/**
	 * Sets the state.
	 *
	 * @param state the new state
	 */
	public synchronized void setState(State state)
	{
		this.state.set(state);
	}
	
	/**
	 * This is a blocked operation.
	 *
	 * @param timeout the timeout
	 * @throws InterruptedException the interrupted exception
	 */
	public synchronized void cancel(long timeout) throws InterruptedException
	{
		State s = state.get();
		if( s== State.IDLE || s == State.DISABLED){
			return;
		}
		this.cancelRequested = true;
		wait(timeout);
		
	}
	
	/**
	 * Checks if is canceled.
	 *
	 * @return true, if is canceled
	 */
	public boolean isCanceled()
	{
		return cancelRequested;
	}
	
	/**
	 * Inits the.
	 *
	 * @param cfg the cfg
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected abstract void init(CacheConfiguration cfg) throws IOException;
	
	
	/**
	 * Loads data from disk to memory syncronously.
	 *
	 * @param pl the pl
	 * @return the off heap cache
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public  abstract OffHeapCache load( ProgressListener pl) throws IOException;
			
	/**
	 * Loads data from disk to memory asyncronously.
	 *
	 * @param pl the pl
	 * @return the off heap cache
	 * @throws IOException Signals that an I/O exception has occurred.
	 */	

	public Future<OffHeapCache> loadAsync( ProgressListener pl)
			throws IOException {
		LoadCacheTask task = new LoadCacheTask(this, pl);		
		new Thread(task, "AsyncLoad").start();		
		return task;
		
	}
	
	/**
	 * Stores (saves) data from memory to disk syncronously.
	 *
	 * @param memStore the mem store
	 * @param ignoreExpired the ignore expired
	 * @param pl the pl
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void store(OffHeapCache memStore, boolean ignoreExpired, ProgressListener pl) 
		throws IOException;

	
	/**
	 * Store fast.
	 *
	 * @param memStore the mem store
	 * @param ignoreExpired the ignore expired
	 * @param pl the pl
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void storeFast(OffHeapCache memStore, boolean ignoreExpired, ProgressListener pl)
		throws IOException
	{
		// default implementation just call 'store'
		store(memStore, ignoreExpired, pl);
	}
	
	/**
	 * Stores (saves) data from memory to disk asyncronously.
	 *
	 * @param memStore the mem store
	 * @param ignoreExpired the ignore expired
	 * @param pl the pl
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void storeAsync(final OffHeapCache memStore, final boolean ignoreExpired,
			final ProgressListener pl) throws IOException {
		Runnable r = new Runnable(){
			public void run()
			{
				try{
					store(memStore, ignoreExpired, pl);
				}catch(IOException e)
				{
					LOG.error(e);
				}
			}
		};
		
		new Thread(r,"AsyncStore").start();


	}
	
	/**
	 * Stores (saves) data from memory to disk (snapshot).
	 * The operation runs in parallel with others and must not
	 * seriously interfere regular operations. Snapshots are async
	 * only.
	 *
	 * @param ignoreExpired the ignore expired
	 * @param pl the pl
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void startSnapshots( boolean ignoreExpired, ProgressListener pl ) 
		throws IOException;
	
	/**
	 * Stop snapshotting.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void stopSnapshots() throws IOException;
	
	/**
	 * Is snapshots enabled.
	 *
	 * @return true if yes
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract boolean isSnapshotsEnabled() throws IOException;  
		
	/**
	 * If need to bypass memory entirely.
	 *
	 * @param keyValue the key value
	 * @param expTime the exp time
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void put (ByteBuffer keyValue, long expTime) throws IOException;
	
	/**
	 * Get value by key.
	 *
	 * @param key the key
	 * @return the cache entry
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void get(ByteBuffer key) throws IOException;
	

	
	/**
	 * Delete.
	 *
	 * @param key the key
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void delete(ByteBuffer key) throws IOException;
	
	/**
	 * Gets the config.
	 *
	 * @return the config
	 */
	public abstract DiskStoreConfiguration getConfig();

	/**
	 * Close.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void close() throws IOException;

	/**
	 * Flush.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public abstract void flush() throws IOException;
	
	/**
	 * Are row operations supported.
	 *
	 * @return true, if successful
	 */
	public abstract boolean areRowOperationsSupported();
	
}
