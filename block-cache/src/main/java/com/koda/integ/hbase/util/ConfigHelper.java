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
package com.koda.integ.hbase.util;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.koda.cache.eviction.EvictionPolicy;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;
import com.koda.config.DiskStoreConfiguration;
import com.koda.persistence.PersistenceMode;
//import com.koda.persistence.leveldb.LevelDBConfiguration;
//import com.koda.persistence.leveldb.LevelDBStore;
import com.koda.persistence.rawfs.RawFSConfiguration;
import com.koda.persistence.rawfs.RawFSStore;


// TODO: Auto-generated Javadoc
/**
 * The Class ConfigHelper.
 */
public class ConfigHelper {

	  /** The Constant LOG. */
  	static final Log LOG = LogFactory.getLog(ConfigHelper.class);
	
	/**
	 * Gets the cache configuration.
	 *
	 * @param cfg the cfg
	 * @return the cache configuration
	 */
	public static CacheConfiguration getCacheConfiguration(Configuration cfg)
	{
		
		CacheConfiguration ccfg = new CacheConfiguration();
		String value = cfg.get(CacheConfiguration.COMPRESSION, "none");
		//TODO not safe
		ccfg.setCodecType(CodecType.valueOf(value.toUpperCase()));
		ccfg.setCompressionThreshold(cfg.getInt(CacheConfiguration.COMPRESSION_THRESHOLD, 100));
		ccfg.setDefaultExpireTimeout(cfg.getInt(CacheConfiguration.DEFAULT_EXPIRE_TIMEOUT, 0));
		ccfg.setEvictOnExpireFirst(cfg.getBoolean(CacheConfiguration.EVICT_EXPIRED_FIRST, true));
		ccfg.setCandidateListSize((cfg.getInt(CacheConfiguration.EVICTION_LIST_SIZE, 30)));
		ccfg.setEvictionPolicy((cfg.get(CacheConfiguration.EVICTION_POLICY, "lru")));
		ccfg.setHighWatermark(cfg.getFloat(CacheConfiguration.HIGH_WATERMARK, 0.98f));
		ccfg.setLowWatermark(cfg.getFloat(CacheConfiguration.LOW_WATERMARK, 0.95f));		
		
		value = cfg.get(CacheConfiguration.KEY_CLASSNAME);
		if(value != null){
			ccfg.setKeyClassName(value);
		}
		
		value = cfg.get(CacheConfiguration.VALUE_CLASSNAME);
		if(value != null){
			ccfg.setValueClassName(value);
		}
		
		ccfg.setMaxConcurrentReaders(cfg.getInt(CacheConfiguration.MAX_CONCURRENT_READERS, 0));
		ccfg.setMaxQueryProcessors(cfg.getInt(CacheConfiguration.MAX_QUERY_PROCESSORS, 0));		
		
		ccfg.setMaxEntries(cfg.getLong(CacheConfiguration.MAX_ENTRIES, 0));
		value = cfg.get(CacheConfiguration.MAX_GLOBAL_MEMORY);
		if( value != null){
			ccfg.setMaxGlobalMemory(Long.parseLong(value));
		} else{
			LOG.info(" Max global memory is not specified.");	
		}
		
		value = cfg.get(CacheConfiguration.MAX_MEMORY);
		if( value != null){
			ccfg.setMaxMemory(Long.parseLong(value));
		} else{
			LOG.info(" Max memory is not specified.");	
		}
		
		ccfg.setCacheName(cfg.get(CacheConfiguration.NAME, "default"));
		
		ccfg.setCacheNamespace(cfg.get(CacheConfiguration.NAMESPACE, "default"));
		
		ccfg.setSerDeBufferSize(cfg.getInt(CacheConfiguration.SERDE_BUFSIZE, 4*1024*1024));
		
		// TODO bucket number must be calculated
		ccfg.setBucketNumber(cfg.getInt(CacheConfiguration.TOTAL_BUCKETS, 1000000));
		
		// Done with common cache configurations
		value = cfg.get(DiskStoreConfiguration.PERSISTENCE, "none");
		if( value.equals("none")){
			// We are done
			return ccfg;
		}
		DiskStoreConfiguration dcfg = loadDiskStoreCfg(cfg, value);
		
		ccfg.setDataStoreConfiguration(dcfg);
		
		return ccfg;
		
	}

	/**
	 * Creates default Configuration with no persistence,
	 * no compression but with Global and Cache max memory set.
	 *
	 * @param maxGlobalSize the max global size
	 * @param maxSize the max size
	 * @return the default config
	 */
	public static Configuration getDefaultConfig(long maxGlobalSize, long maxSize)
	{
		Configuration cfg = new Configuration();
		cfg.setLong(CacheConfiguration.MAX_GLOBAL_MEMORY, maxGlobalSize);
		cfg.setLong(CacheConfiguration.MAX_MEMORY, maxSize);
		return cfg;
	}
	
	/**
	 * Creates default Configuration with  persistence,
	 * compression but with Global and.
	 *
	 * @param maxGlobalSize the max global size
	 * @param maxSize the max size
	 * @param mode the mode
	 * @param codec the codec
	 * @return the default config more
	 */
	public static Configuration getDefaultConfigMore(long maxGlobalSize, long maxSize, PersistenceMode mode,
			CodecType codec)
	{
		Configuration cfg = new Configuration();
		cfg.setLong(CacheConfiguration.MAX_GLOBAL_MEMORY, maxGlobalSize);
		cfg.setLong(CacheConfiguration.MAX_MEMORY, maxSize);
		cfg.set(CacheConfiguration.COMPRESSION, codec.toString());
		//cfg.set(name, value);
		return cfg;
	}
	
	/**
	 * Load disk store cfg.
	 *
	 * @param cfg the cfg
	 * @param value the value
	 * @return the disk store configuration
	 */
	private static DiskStoreConfiguration loadDiskStoreCfg(Configuration cfg,
			String value) {
		DiskStoreConfiguration diskCfg = null;
		PersistenceMode mode = PersistenceMode.valueOf(value);
		switch(mode){
			case ONDEMAND:
			case SNAPSHOT:
				diskCfg = new RawFSConfiguration();
				diskCfg.setDiskStoreImplementation(RawFSStore.class);
				diskCfg.setStoreClassName(RawFSStore.class.getName());
				
				break;
//			case WRITE_AROUND:
//			case WRITE_BEHIND:
//			case WRITE_THROUGH:
//				diskCfg = new LevelDBConfiguration();
//				diskCfg.setDiskStoreImplementation(LevelDBStore.class);
//				diskCfg.setStoreClassName(LevelDBStore.class.getName());
//				break;
		}
		
		diskCfg.setPersistenceMode(mode);
		
		String val = cfg.get(DiskStoreConfiguration.DATA_DIRS);
		if(val == null){
			LOG.fatal("\'"+DiskStoreConfiguration.DATA_DIRS+"\' is not specified. Aborted.");
			throw new RuntimeException("\'"+DiskStoreConfiguration.DATA_DIRS+"\' is not specified. Aborted.");
		}
		diskCfg.setDbDataStoreRoots(val.split(","));
		
		diskCfg.setStoreName(cfg.get(DiskStoreConfiguration.NAME, "default")); // DB name/ Subfolder in a root
		
		diskCfg.setDbSnapshotInterval(cfg.getLong(DiskStoreConfiguration.SNAPSHOT_INTERVAL, 3600000)); 
		
		diskCfg.setDbCompressionType(CodecType.valueOf(cfg.get(DiskStoreConfiguration.COMPRESSION, "none").toUpperCase())); 
		
		diskCfg.setDiskStoreMaxSize(cfg.getLong(DiskStoreConfiguration.STORE_SIZE_LIMIT, 0)); 
		
		diskCfg.setDiskStoreEvictionPolicy(EvictionPolicy.valueOf(cfg.get(DiskStoreConfiguration.EVICTION_POLICY, "none")));
		
		diskCfg.setDiskStoreEvictionHighWatermark(cfg.getFloat(DiskStoreConfiguration.EVICTION_HIGHWATERMARK, 0.98f));
		
		diskCfg.setDiskStoreEvictionLowWatermak(cfg.getFloat(DiskStoreConfiguration.EVICTION_LOWWATERMARK, 0.95f));
				
		diskCfg = loadSpecific(cfg, diskCfg);
		
		return diskCfg;
	}
	
	/**
	 * Load specific.
	 *
	 * @param cfg the cfg
	 * @param dcfg the dcfg
	 * @return the disk store configuration
	 */
	private static DiskStoreConfiguration loadSpecific (Configuration cfg, DiskStoreConfiguration dcfg)
	{
		if( dcfg instanceof RawFSConfiguration){
			return loadRawFSSpecific( cfg, (RawFSConfiguration) dcfg);
		} /*else if( dcfg instanceof LevelDBConfiguration){
			return loadLevelDBSpecific( cfg, (LevelDBConfiguration) dcfg);
		}*/
		return dcfg;
	}

	/**
	 * Load level db specific.
	 *
	 * @param cfg the cfg
	 * @param dcfg the dcfg
	 * @return the disk store configuration
	 */
//	private static DiskStoreConfiguration loadLevelDBSpecific(
//			Configuration cfg, LevelDBConfiguration dcfg) {
//		
//		String value = null;
//		
//		value = cfg.get(LevelDBConfiguration.WRITE_BATCH_SIZE);
//		if(value != null){
//			dcfg.setDbWriteBatchSize(Integer.parseInt(value));
//		}
//				
//		value = cfg.get(LevelDBConfiguration.WRITE_BATCH_INTERVAL);
//		if(value != null){
//			dcfg.setDbWriteBatchInterval(Integer.parseInt(value));
//		}
//
//		
//		value = cfg.get(LevelDBConfiguration.SYNC_AFTER_BATCH);
//		if(value != null){
//			dcfg.setDbSyncAfterBatch(Boolean.parseBoolean(value));
//		}
//		
//		
//		value = cfg.get(LevelDBConfiguration.CREATE_IF_MISSING);
//		if(value != null){
//			dcfg.setDbCreateIfMissing(Boolean.parseBoolean(value));
//		}
//		
//		value = cfg.get(LevelDBConfiguration.ERROR_IF_EXISTS);
//		if(value != null){
//			dcfg.setDbErrorIfExists(Boolean.parseBoolean(value));
//		}
//		
//		
//		value = cfg.get(LevelDBConfiguration.PARANOID_CHECKS);
//		if(value != null){
//			dcfg.setDbParanoidChecks(Boolean.parseBoolean(value));
//		}
//
//		value = cfg.get(LevelDBConfiguration.WRITE_BUFFER_SIZE);
//		if(value != null){
//			dcfg.setDbWriteBufferSize(Integer.parseInt(value));
//		}
//
//		
//		value = cfg.get(LevelDBConfiguration.MAX_OPEN_FILES);
//		if(value != null){
//			dcfg.setDbMaxOpenFiles(Integer.parseInt(value));
//		}
//		
//		value = cfg.get(LevelDBConfiguration.BLOCK_CACHE);
//		if(value != null){
//			dcfg.setDbBlockCache(Boolean.parseBoolean(value));
//		}
//		
////		LevelDBConfiguration.BLOCK_CACHE_SIZE ;
////		
////		value = cfg.get(LevelDBConfiguration.BLOCK_CACHE_SIZE);
////		if(value != null){
////			dcfg.setDbBl(Integer.parseInt(value));
////		}
//		
//		value = cfg.get(LevelDBConfiguration.BLOCK_SIZE);
//		if(value != null){
//			dcfg.setDbBlockSize(Integer.parseInt(value));
//		}
//		
//		value = cfg.get(LevelDBConfiguration.BLOCK_RESTART_INTERVAL);
//		if(value != null){
//			dcfg.setDbBlockRestartInterval(Integer.parseInt(value));
//		}
//		
//		return dcfg;
//	}

	/**
	 * Load raw fs specific.
	 *
	 * @param cfg the cfg
	 * @param dcfg the dcfg
	 * @return the disk store configuration
	 */
	private static DiskStoreConfiguration loadRawFSSpecific(Configuration cfg,
			RawFSConfiguration dcfg) {
		
		String value = cfg.get(RawFSConfiguration.IO_THREADS);
		if(value != null){
			dcfg.setTotalIOThreads(Integer.parseInt(value));
		} else{
			dcfg.setTotalIOThreads(dcfg.getDbDataStoreRoots().length);
		}
		
		value = cfg.get(RawFSConfiguration.WORKER_THREADS);
		if(value != null){
			dcfg.setTotalWorkerThreads(Integer.parseInt(value));
		} else{
			dcfg.setTotalIOThreads(Runtime.getRuntime().availableProcessors());
		}
		
		dcfg.setRWBufferSize(cfg.getInt(RawFSConfiguration.RW_BUFFER_SIZE, 4*1024*1024));
		
		return dcfg;
	}	
	
	public static Configuration copy(Configuration cfg)
	{
		Iterator<Entry<String,String>> it = cfg.iterator();
		Configuration copy = new Configuration();
		while(it.hasNext()){
			Entry<String, String> next = it.next();
			copy.set(next.getKey(), next.getValue());
		}
		return copy;
	}
	
}
