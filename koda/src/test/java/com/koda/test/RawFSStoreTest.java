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
package com.koda.test;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.koda.KodaException;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;
import com.koda.persistence.DefaultProgressListener;
import com.koda.persistence.DiskStore;
import com.koda.persistence.PersistenceMode;
import com.koda.persistence.ProgressListener;
import com.koda.persistence.rawfs.RawFSConfiguration;
import com.koda.persistence.rawfs.RawFSStore;
import com.koda.util.Utils;

// TODO: Auto-generated Javadoc
/**
 * The Class RawFSStoreTest.
 */
public class RawFSStoreTest {
	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(RawFSStoreTest.class);
	
	/** The N. */
	//static int N = 10000000;
	
	/** The M. */
	static int M = 40000;
	
	/** The s memory limit. */
	static long sMemoryLimit = 15000000000L;
	
	/** The s cache. */
	static OffHeapCache sCache;
	
	/** The s client threads. */
	static int sClientThreads = 1;
	
	/** The s config. */
	static CacheConfiguration sConfig;
	
	/** The test dir. */
	static String[] testDir = new String[]{"/tmp/ramdisk/data1"};// , "/tmp/ramdisk/data2", "/tmp/ramdisk/data3", "/tmp/ramdisk/data4" } ;
	
	
	/**
	 * Rmdir.
	 *
	 * @param dir the dir
	 */
	@SuppressWarnings("unused")
    private static void rmdir(File dir)
	{

		if(dir.isFile()){
			dir.delete();
			return;
		}
		File[] list = dir.listFiles();
		for(File f: list){
			rmdir(f);
		}
	}
	
	/**
	 * Inits the cache kode.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws KodaException the koda exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void initCacheKoda() throws NativeMemoryException, KodaException, IOException {
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		sConfig = new com.koda.config.CacheConfiguration();
		sConfig.setBucketNumber(M);
		sConfig.setMaxMemory(sMemoryLimit);
		sConfig.setEvictionPolicy("LRU");
		sConfig.setDefaultExpireTimeout(6000);
		sConfig.setCacheName("test");
		//sConfig.setCompressionEnabled(true);
		sConfig.setCodecType(CodecType.LZ4);
		//sConfig.setKeyClassName(byte[].class.getName());
		//sConfig.setValueClassName(byte[].class.getName());
		RawFSConfiguration storeConfig = new RawFSConfiguration();
		storeConfig.setDiskStoreImplementation(RawFSStore.class);
		storeConfig.setDbDataStoreRoots(testDir);
		storeConfig.setPersistenceMode(PersistenceMode.ONDEMAND);
		storeConfig.setDbCompressionType(CodecType.LZ4);
		storeConfig.setDbSnapshotInterval(15);
		//storeConfig.setTotalWorkerThreads(4);
		//storeConfig.setTotalIOThreads(2);
		
		sConfig.setDataStoreConfiguration(storeConfig);
		
		sCache = manager.getCache(sConfig, new ProgressListener(){
			@Override
			public void canceled() { LOG.info("Canceled");}
			@Override
			public void error(Throwable t, boolean aborted) {
				LOG.error("Aborted="+aborted, t);				
			}
			@Override
			public void finished() {
				LOG.info("Finished");				
			}
			@Override
			public void progress(long done, long total) {
				//LOG.info("Loaded "+done +" out of "+total);
			}
			@Override
			public void started() {
				LOG.info("Started");
			}
		});
		
	}
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws NativeMemoryException the native memory exception
	 * @throws KodaException the koda exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String args[]) throws NativeMemoryException, KodaException, IOException
	{
		long time;
		
		initCacheKoda();		
		LOG.info("Init done. Cache size ="+sCache.size());
		if(sCache == null || sCache.size()==0){
			time = System.currentTimeMillis();
			loadData();
			LOG.info("Loading done in "+(System.currentTimeMillis() - time)+ " size="+ sCache.getTotalAllocatedMemorySize());
		}
		time = System.currentTimeMillis();
		verifyLoad();
		LOG.info("Verification done in "+(System.currentTimeMillis()- time)+"ms");
		saveCache();
		
		time = System.currentTimeMillis();
		
		sCache.clear();
		
		LOG.info("Cache cleared in "+(System.currentTimeMillis()- time)+"ms");
		
		
		DiskStore store = DiskStore.getDiskStore(sConfig);
		
		time = System.currentTimeMillis();
		
		ProgressListener pl = new ProgressListener(){

			@Override
			public void canceled() {
				LOG.info("Canceled");
				
			}

			@Override
			public void error(Throwable t, boolean aborted) {
				LOG.error("Aborted="+aborted, t);
				
			}

			@Override
			public void finished() {
				LOG.info("Finished");
				
			}

			@Override
			public void progress(long done, long total) {
				//LOG.info("Loaded "+done +" out of "+total);
				
			}

			@Override
			public void started() {
				LOG.info("Started");
				
			}
			
		};
		
		OffHeapCache cache = null;
			
			if(store instanceof RawFSStore){
				cache = ((RawFSStore) store).loadFast(pl);
			} else{
				cache = store.load(pl);	
			}
		
		LOG.info("Loading cache done in "+(System.currentTimeMillis() - time)+" ms. Cache size="+cache.size());
		
		
//		try {
//			testSnapshots();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
	}
	
	
	
	
	/**
	 * Save cache.
	 *
	 * @throws KodaException the koda exception
	 */
	private static void saveCache() throws KodaException {
		long start = System.currentTimeMillis();
		sCache.save(new ProgressListener(){

			@Override
			public void canceled() {
				LOG.info("Canceled");
				
			}

			@Override
			public void error(Throwable t, boolean aborted) {
				LOG.error("Aborted="+aborted, t);
				
			}

			@Override
			public void finished() {
				LOG.info("Finished");
				
			}

			@Override
			public void progress(long done, long total) {
				//LOG.info("Saved "+done +" out of "+total);
				
			}

			@Override
			public void started() {
				LOG.info("Started");
				
			}
			
			
		});
		LOG.info("Saved "+sCache.getAllocatedMemorySize()+" in "+(System.currentTimeMillis()-start)+"ms");
	}
	
	
	/**
	 * Test snapshots.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws InterruptedException the interrupted exception
	 */
	@SuppressWarnings("unused")
    private static void _testSnapshots() throws IOException, InterruptedException
	{
		DiskStore store = sCache.getDiskStore();
				
		store.startSnapshots( true, new DefaultProgressListener());

		Thread.sleep(sCache.getDiskStore().getConfig().getDbSnapshotInterval()*5000);
		
		store.stopSnapshots();
		LOG.info("Test snapshots finished");
		
	}
	
	/**
	 * Load data.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void loadData() throws NativeMemoryException, IOException {
		byte[] keyBase = new byte[32];
		byte[] valueBase = new byte[32];
		
		for(int i=0; i < M; i++)
		{
			byte[] key = get(keyBase, i);
			byte[] value = get(valueBase, M-i);
			sCache.put(key, value);			

		}		
		
	}
	
	
	/**
	 * Verify load.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

    private static void verifyLoad() throws NativeMemoryException, IOException
	{
		byte[] keyBase = new byte[32];
		byte[] valueBase = new byte[32];
		long failed = 0;
		for(int i=0; i < M; i++)
		{
			byte[] key = get(keyBase, i);
			byte[] value = get(valueBase, M-i);
			byte[] val = (byte[]) sCache.get(key);
			if(val == null || !Utils.equals(val, value)){
				LOG.error("FAILED for i="+i);
				failed++;
			}
			
		}	
		LOG.info("Verification complete. Failed="+failed +" out from "+ M);
	}
	
	/**
	 * Gets the.
	 *
	 * @param buf the buf
	 * @param i the i
	 * @return the byte[]
	 */
	private static byte[] get(byte[] buf, int i) {
	  for(int k=0; k < buf.length; k++){
	    buf[k] = (byte) i;
	  }
	  
		buf[buf.length - 4] = (byte) (i >>> 24);
		buf[buf.length - 3] = (byte) (i >>> 16);
		buf[buf.length - 2] = (byte) (i >>> 8);
		buf[buf.length - 1] = (byte) (i);
		
		return buf;
	}
}
