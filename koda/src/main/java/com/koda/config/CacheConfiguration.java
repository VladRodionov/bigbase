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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.koda.compression.CodecType;
import com.koda.persistence.PersistenceMode;

// TODO: Auto-generated Javadoc
/**
 * The Class CacheConfiguration.
 */
public class CacheConfiguration implements Cloneable, Defaults{
	/** The Constant LOG. */
    
	public final static String MAX_QUERY_PROCESSORS = "koda.cache.max.query.processors";
	
	/** The Constant MAX_CONCURRENT_READERS. */
	public final static String MAX_CONCURRENT_READERS = "koda.cache.max.concurrent.readers";
	
	/** The Constant TOTAL_BUCKETS. */
	public final static String  TOTAL_BUCKETS = "koda.cache.total.buckets";
	
	/** The Constant MAX_ENTRIES. */
	public final static String  MAX_ENTRIES = "koda.cache.max.entries";
	
	/** The Constant MAX_MEMORY. */
	public final static String  MAX_MEMORY = "koda.cache.max.memory.limit";

	/** The Constant MAX_GLOBAL_MEMORY. */
	public final static String  MAX_GLOBAL_MEMORY = "koda.cache.max.global.memory.limit";
	
	/** The Constant HIGH_WATERMARK. */
	public final static String  HIGH_WATERMARK = "koda.cache.high.watermark";
	
	/** The Constant LOW_WATERMARK. */
	public final static String  LOW_WATERMARK = "koda.cache.low.watermark";
	
	/** The Constant EVICTION_POLICY. */
	public final static String EVICTION_POLICY = "koda.cache.eviction.policy";
	
	/** The Constant NAME. */
	public final static String NAME = "koda.cache.name";
	
	/** The Constant NAMESPACE. */
	public final static String NAMESPACE = "koda.cache.namespace";
	
	/** The Constant EVICTION_LIST_SIZE. */
	public final static String EVICTION_LIST_SIZE = "koda.cache.eviction.list.size";
	
	/** The Constant EVICT_EXPIRED_FIRST. */
	public final static String EVICT_EXPIRED_FIRST = "koda.cache.evict.expired.first";
	
	/** The Constant DEFAULT_EXPIRE_TIMEOUT. */
	public final static String DEFAULT_EXPIRE_TIMEOUT = "koda.cache.default.expire.timeout";
	
	/** The Constant SERDE_BUFSIZE. */
	public final static String SERDE_BUFSIZE = "koda.cache.serde.bufsize";
	
	/** The Constant COMPRESSION. */
	public final static String COMPRESSION = "koda.cache.compression"; // codec
	
	/** The Constant COMPRESSION_THRESHOLD. */
	public final static String COMPRESSION_THRESHOLD = "koda.cache.compression.threshold";
	
	/** The Constant COMPRESSION_LEVEL. */
	public final static String COMPRESSION_LEVEL = "koda.cache.compression.level";
	
	/** The Constant KEY_CLASSNAME. */
	public final static String KEY_CLASSNAME = "koda.cache.key.classname";
	
	/** The Constant VALUE_CLASSNAME. */
	public final static String VALUE_CLASSNAME = "koda.cache.value.classname";
	
	/** The Constant MALLOC_CONCURRENT. */
	public final static String MALLOC_CONCURRENT = "koda.malloc.concurrent";
	
	/** The Constant MALLOC_DEBUG. */
	public final static String MALLOC_DEBUG = "koda.malloc.debug";
	
    /** The Constant MEMOPS_DEBUG. */
    public final static String MEMOPS_DEBUG = "koda.memops.debug";	
    
    /** The Constant MALLOC_CONCURRENCY. */
    public final static String MALLOC_CONCURRENCY = "koda.malloc.concurrency";
	
	/** The Constant DISKSTORE_CONFIG. */
	public final static String DISKSTORE_CONFIG = "koda.diskstore.config";
	
	/** The Constant HISTOGRAM_ENABLED. */
	public final static String HISTOGRAM_ENABLED = "koda.cache.histogram.enabled";	
	
	/** The Constant HISTOGRAM_BINS. */
	public final static String HISTOGRAM_BINS = "koda.cache.histogram.enabled.bins";	
	
	/** The Constant HISTOGRAM_SAMPLES. */
	public final static String HISTOGRAM_SAMPLES = "koda.cache.histogram.samples";	
	
	/** The Constant HISTOGRAM_UPDATE_INTERVAL. */
	public final static String HISTOGRAM_UPDATE_INTERVAL = "koda.cache.histogram.update.interval";
	
	/** The Constant HISTOGRAM_LOG_ENABLED. */
	public final static String HISTOGRAM_LOG_ENABLED = "koda.cache.histogram.log.enabled";
	
	/** For LRU2Q. */
	public final static String LRU2Q_INSERT_POINT = "koda.cache.lru2q.insert.point";
	
	/** General for eviction. */
	public final static String PREEVICTION_LIST_SIZE = "koda.cache.preeviction.list.size";
	
	public final static String EVICTION_ATTEMPTS = "koda.cache.eviction.attempts";
	
	public final static String OPTIMIZER_THREADS = "koda.cache.optimizer.threads";
	
	public final static String OPTIMIZER_LEVEL  = "koda.cache.optimizer.level";
	
	public final static String OPTIMIZER_ENABLED = "koda.cache.optimizer.enabled";
	
	/** Default values specific to Cache. */

		
	public final static String DEFAULT_BUCKET_NUMBER = "10000000";
	   
    /** The Constant DEFAULT_CACHE_NAMESPACE. */
    public final static String DEFAULT_CACHE_NAMESPACE = "default";
    
    /** The Constant DEFAULT_NAME. */
    public final static String DEFAULT_NAME = "name";
    
    /** The Constant DEFAULT_EVICTION_CANDIDATE_LIST_SIZE. */
    public final static String DEFAULT_EVICTION_CANDIDATE_LIST_SIZE = "30";
    
    public final static String DEFAULT_EVICTION_ATTEMTS = "10";
    
    /** The Constant DEFAULT_SERDE_BUFFER_SIZE. */
    public final static String DEFAULT_SERDE_BUFFER_SIZE = "4194304";    
    
    /** The Constant DEFAULT_COMPRESSION_LEVEL. */
    public final static String DEFAULT_COMPRESSION_LEVEL = "0";
    
    /** The Constant DEFAULT_COMPRESSION_THRESHOLD. */
    public final static String DEFAULT_COMPRESSION_THRESHOLD = "100";
    
    /** The Constant DEFAULT_HISTOGRAM_ENABLED. */
    public final static String DEFAULT_HISTOGRAM_ENABLED = "false";
    
    /** The Constant DEFAULT_HISTOGRAM_BINS. */
    public final static String DEFAULT_HISTOGRAM_BINS = "100";
    
    /** The Constant DEFAULT_HISTOGRAM_SAMPLES. */
    public final static String DEFAULT_HISTOGRAM_SAMPLES = "2000";
    
    /** The Constant DEFAULT_HISTOGRAM_UPDATE_INTERVAL. */
    public final static String DEFAULT_HISTOGRAM_UPDATE_INTERVAL = "1000";
    
    /** The Constant DEFAULT_HISTOGRAM_LOG_ENABLED. */
    public final static String DEFAULT_HISTOGRAM_LOG_ENABLED = "false";
        
    /** The Constant DEFAULT_LRU2Q_INSERT_POINT. */
    public final static String DEFAULT_LRU2Q_INSERT_POINT = "0.5";    
            
    /** The Constant DEFAULT_PREEVICTION_LIST_SIZE. */
    public final static String DEFAULT_PREEVICTION_LIST_SIZE = "20";
    
    public final static String DEFAULT_OPTIMIZER_THREADS = "1";
    /**
     *  Number between 1-5 (inclusive). 
     */
    public final static String DEFAULT_OPTIMIZER_LEVEL  = "1";
    
    public final static String DEFAULT_OPTIMIZER_ENABLED = "false";
    
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(CacheConfiguration.class);

	
	/** The m data store config. */
	private DiskStoreConfiguration mDataStoreConfig;
	
	
	/** The m properties. */
	private Properties mProperties = new Properties() ;
	/**
	 * Instantiates a new cache configuration.
	 */
	public CacheConfiguration()
	{		
	}

	/**
	 * Load.
	 *
	 * @param is the is
	 * @return the cache configuration
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static CacheConfiguration load(InputStream is) throws IOException
	{
	   CacheConfiguration cfg = new CacheConfiguration();	   
	   cfg.mProperties.load(is);
	   // Load disk store configuration
	   String fileName = cfg.getDataStoreConfigFileName();
	   if(fileName != null){
	       FileInputStream fis = new FileInputStream(fileName);
	       DiskStoreConfiguration dconf = DiskStoreConfiguration.load(fis);
	       cfg.setDataStoreConfiguration(dconf);
	       fis.close();
	   }
	   return cfg;
	}

	/**
	 * Store.
	 *
	 * @param os the os
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void store(OutputStream os) throws IOException
	{
	        
	    LOG.info("store:: Saving cache configuration");
	    mProperties.store(os, "Koda Cache configuration. Stored on "+new Date());
	    DiskStoreConfiguration dsc = getDiskStoreConfiguration();
	    if(dsc != null){
	        FileOutputStream fos = new FileOutputStream(getDataStoreConfigFileName());
	        dsc.store(fos, "Koda Disk Store configuration. Stored on "+new Date());
	        fos.close();
	    }
	    LOG.info("store:: Done");
	}
	
	/**
	 * Gets the key class.
	 *
	 * @return the key class
	 */
	public String getKeyClassName()
	{
		return mProperties.getProperty(KEY_CLASSNAME, DEFAULT_NONE);
	}
	
	/**
	 * Sets the key class.
	 *
	 * @param clsName the new key class name
	 */
	public void setKeyClassName(String clsName)
	{
	    mProperties.setProperty(KEY_CLASSNAME, clsName);
	}
	
	
	/**
	 * Gets the value class.
	 *
	 * @return the value class
	 */
	public String getValueClassName()
	{
	    return mProperties.getProperty(VALUE_CLASSNAME, DEFAULT_NONE);
	}
	
	/**
	 * Sets the value class.
	 *
	 * @param clsName the new value class name
	 */
	public void setValueClassName(String clsName)
	{
	    mProperties.setProperty(VALUE_CLASSNAME, clsName);
	}
	
	/**
	 * Checks if is persistent.
	 *
	 * @return true, if is persistent
	 */
	public boolean isPersistent()
	{
		return mDataStoreConfig != null && mDataStoreConfig.getPersistenceMode() != PersistenceMode.NONE;
	}
	
	/**
	 * Gets the data store configuration.
	 *
	 * @return the data store configuration
	 */
	public DiskStoreConfiguration getDiskStoreConfiguration()
	{
		return mDataStoreConfig;
	}
	
	/**
	 * Sets the data store configuration.
	 *
	 * @param cfg the new data store configuration
	 */
	public void setDataStoreConfiguration(DiskStoreConfiguration cfg)
	{
		this.mDataStoreConfig = cfg;
	}
	
	/**
	 * Gets the codec type.
	 *
	 * @return the codec type
	 */
	public CodecType getCodecType()
	{		
		return CodecType.valueOf(mProperties.getProperty(COMPRESSION, DEFAULT_NONE).toUpperCase());
	}
	
	/**
	 * Sets the codec type.
	 *
	 * @param type the new codec type
	 */
	public void setCodecType(CodecType type)
	{
		mProperties.setProperty(COMPRESSION, type.name());
	}
	
	/**
	 * Gets the max query processors.
	 *
	 * @return the max query processors
	 */
	public int getMaxQueryProcessors() {
		return Integer.parseInt(mProperties.getProperty(MAX_QUERY_PROCESSORS, DEFAULT_UNLIMITED));
	}


	/**
	 * Sets the max query processors.
	 *
	 * @param maxQueryProcessors the new max query processors
	 */
	public void setMaxQueryProcessors(int maxQueryProcessors) {
	    mProperties.setProperty(MAX_QUERY_PROCESSORS, Integer.toString(maxQueryProcessors));
	}


	/**
	 * Gets the max concurrent readers.
	 *
	 * @return the max concurrent readers
	 */
	public int getMaxConcurrentReaders() {
	    return Integer.parseInt(mProperties.getProperty(MAX_CONCURRENT_READERS, DEFAULT_UNLIMITED));
	}


	/**
	 * Sets the max concurrent readers.
	 *
	 * @param maxConcurrentReaders the new max concurrent readers
	 */
	public void setMaxConcurrentReaders(int maxConcurrentReaders) {
	    mProperties.setProperty(MAX_CONCURRENT_READERS, Integer.toString(maxConcurrentReaders));
	}


	/**
	 * Gets the bucket number.
	 *
	 * @return the bucket number
	 */
	public int getBucketNumber() {
	    return Integer.parseInt(mProperties.getProperty(TOTAL_BUCKETS, DEFAULT_BUCKET_NUMBER));
	}


	/**
	 * Sets the bucket number.
	 *
	 * @param bucketNumber the new bucket number
	 */
	public void setBucketNumber(int bucketNumber) {
	    mProperties.setProperty(TOTAL_BUCKETS, Integer.toString(bucketNumber));
	}


	/**
	 * Gets the max entries.
	 *
	 * @return the max entries
	 */
	public long getMaxEntries() {
	    return Integer.parseInt(mProperties.getProperty(MAX_ENTRIES, DEFAULT_UNLIMITED));
	}


	/**
	 * Sets the max entries.
	 *
	 * @param maxEntries the new max entries
	 */
	public void setMaxEntries(long maxEntries) {
	    mProperties.setProperty(MAX_ENTRIES, Long.toString(maxEntries));
	}


	/**
	 * Gets the max memory.
	 *
	 * @return the max memory
	 */
	public long getMaxMemory() {
	    return Long.parseLong(mProperties.getProperty(MAX_MEMORY, DEFAULT_UNLIMITED));
	}


	/**
	 * Sets the max memory.
	 *
	 * @param maxMemory the new max memory
	 */
	public void setMaxMemory(long maxMemory) {
	    mProperties.setProperty(MAX_MEMORY, Long.toString(maxMemory));
	}


	/**
	 * Gets the high watermark.
	 *
	 * @return the high watermark
	 */
	public float getHighWatermark() {
	    return Float.parseFloat(mProperties.getProperty(HIGH_WATERMARK, DEFAULT_HIGH_WATERMARK));
	}


	/**
	 * Sets the high watermark.
	 *
	 * @param highWatermark the new high watermark
	 */
	public void setHighWatermark(float highWatermark) {
	    mProperties.setProperty(HIGH_WATERMARK, Float.toString(highWatermark));
	}


	/**
	 * Gets the low watermark.
	 *
	 * @return the low watermark
	 */
	public float getLowWatermark() {
	    return Float.parseFloat(mProperties.getProperty(LOW_WATERMARK, DEFAULT_LOW_WATERMARK));
	}


	/**
	 * Sets the low watermark.
	 *
	 * @param lowWatermark the new low watermark
	 */
	public void setLowWatermark(float lowWatermark) {
	    mProperties.setProperty(LOW_WATERMARK, Float.toString(lowWatermark));
	}


	/**
	 * Gets the eviction policy.
	 *
	 * @return the eviction policy
	 */
	public String getEvictionPolicy() {
		return mProperties.getProperty(EVICTION_POLICY, DEFAULT_NONE);
	}


	/**
	 * Sets the eviction policy.
	 *
	 * @param evictionPolicy the new eviction policy
	 */
	public void setEvictionPolicy(String evictionPolicy) {
		mProperties.setProperty(EVICTION_POLICY, evictionPolicy);
	}


	/**
	 * Gets the cache name.
	 *
	 * @return the cache name
	 */
	public String getCacheName() {
	    return mProperties.getProperty(NAME, DEFAULT_NAME);
	}


	/**
	 * Sets the cache name.
	 *
	 * @param cacheName the new cache name
	 */
	public void setCacheName(String cacheName) {
		mProperties.setProperty(NAME, cacheName);
	}


	/**
	 * Gets the cache namespace.
	 *
	 * @return the cache namespace
	 */
	public String getCacheNamespace() {
	    return mProperties.getProperty(NAMESPACE, DEFAULT_CACHE_NAMESPACE);
	}


	/**
	 * Gets the qualified name.
	 *
	 * @return the qualified name
	 */
	public String getQualifiedName()
	{
		return getCacheNamespace() + "_"+getCacheName();
	}
	
	/**
	 * Sets the cache namespace.
	 *
	 * @param cacheNamespace the new cache namespace
	 */
	public void setCacheNamespace(String cacheNamespace) {
	    mProperties.setProperty(NAMESPACE, cacheNamespace);
	}


	/**
	 * Gets the candidate list size.
	 *
	 * @return the candidate list size
	 */
	public int getCandidateListSize() {
		return Integer.parseInt(mProperties.getProperty(EVICTION_LIST_SIZE, DEFAULT_EVICTION_CANDIDATE_LIST_SIZE));
	}


	/**
	 * Sets the candidate list size.
	 *
	 * @param listSize the new candidate list size
	 */
	public void setCandidateListSize(int listSize) {
		mProperties.setProperty(EVICTION_LIST_SIZE, Integer.toString(listSize));
	}


	/**
	 * Checks if is evict on expire first.
	 *
	 * @return true, if is evict on expire first
	 */
	public boolean isEvictOnExpireFirst() {
		return Boolean.parseBoolean(mProperties.getProperty(EVICT_EXPIRED_FIRST, TRUE));
	}


	/**
	 * Sets the evict on expire first.
	 *
	 * @param evictOnExpireFirst the new evict on expire first
	 */
	public void setEvictOnExpireFirst(boolean evictOnExpireFirst) {
	    mProperties.setProperty(EVICT_EXPIRED_FIRST, Boolean.toString(evictOnExpireFirst));
	}


	/**
	 * Gets the default expire timeout.
	 *
	 * @return the default expire timeout
	 */
	public int getDefaultExpireTimeout() {
	    return Integer.parseInt(mProperties.getProperty(DEFAULT_EXPIRE_TIMEOUT, DEFAULT_ZERO));
	}


	/**
	 * Sets the default expire timeout.
	 *
	 * @param defaultExpireTimeout the new default expire timeout
	 */
	public void setDefaultExpireTimeout(int defaultExpireTimeout) {
	    mProperties.setProperty(DEFAULT_EXPIRE_TIMEOUT, Integer.toString(defaultExpireTimeout));
	}


	/**
	 * Gets the ser de buffer size.
	 *
	 * @return the ser de buffer size
	 */
	public int getSerDeBufferSize() {
	    return Integer.parseInt(mProperties.getProperty(SERDE_BUFSIZE, DEFAULT_SERDE_BUFFER_SIZE));
	}


	/**
	 * Sets the ser de buffer size.
	 *
	 * @param serDeBufferSize the new ser de buffer size
	 */
	public void setSerDeBufferSize(int serDeBufferSize) {
	    mProperties.setProperty(SERDE_BUFSIZE, Integer.toString(serDeBufferSize));
	}


	/**
	 * Checks if is compression enabled.
	 *
	 * @return true, if is compression enabled
	 */
	public boolean isCompressionEnabled() {
		return getCodecType() != CodecType.NONE;
	}


	/**
	 * Sets the compression threshold.
	 *
	 * @param compressionThreshold the new compression threshold
	 */
	public void setCompressionThreshold(int compressionThreshold) {
	    mProperties.setProperty(COMPRESSION_THRESHOLD, Integer.toString(compressionThreshold));
	}


	/**
	 * Gets the compression threshold.
	 *
	 * @return the compression threshold
	 */
	public int getCompressionThreshold() {
		return Integer.parseInt(mProperties.getProperty(COMPRESSION_THRESHOLD, DEFAULT_COMPRESSION_THRESHOLD));
	}


	/**
	 * Sets the max global memory.
	 *
	 * @param maxGlobalMemory the new max global memory
	 */
	public void setMaxGlobalMemory(long maxGlobalMemory) {
	    mProperties.setProperty(MAX_GLOBAL_MEMORY, Long.toString(maxGlobalMemory));
	}


	/**
	 * Gets the max global memory.
	 *
	 * @return the max global memory
	 */
	public long getMaxGlobalMemory() {
		return Long.parseLong(mProperties.getProperty(MAX_GLOBAL_MEMORY, DEFAULT_UNLIMITED));
	}
	
    /**
     * Copy.
     *
     * @return the cache configuration
     * @throws CloneNotSupportedException the clone not supported exception
     */
    public CacheConfiguration copy() throws CloneNotSupportedException
    {
    	CacheConfiguration cloned = (CacheConfiguration) clone();
    	if(mDataStoreConfig != null){
    		cloned.setDataStoreConfiguration( mDataStoreConfig.copy());
    	}
    	return cloned;
    }


	/**
	 * Sets the compression level.
	 *
	 * @param level the new compression level
	 */
	public void setCompressionLevel(int level) {
		mProperties.setProperty(COMPRESSION_LEVEL, Integer.toString(level));
	}


	/**
	 * Gets the compression level.
	 *
	 * @return the compression level
	 */
	public int getCompressionLevel() {
		return Integer.parseInt(mProperties.getProperty(COMPRESSION_LEVEL, DEFAULT_COMPRESSION_LEVEL));
	}
	
	/**
	 * Checks if is malloc concurrent.
	 *
	 * @return true, if is malloc concurrent
	 */
	public boolean isMallocConcurrent()
	{
	    return Boolean.parseBoolean(mProperties.getProperty(MALLOC_CONCURRENT, FALSE));
	}
	
	/**
	 * Sets the malloc concurrent.
	 *
	 * @param v the new malloc concurrent
	 */
	public void setMallocConcurrent(boolean v)
	{	    
	    mProperties.setProperty(MALLOC_CONCURRENT, Boolean.toString(v));
	    System.setProperty(MALLOC_CONCURRENT, Boolean.toString(v));
	}

    /**
     * Sets the data store config file name.
     *
     * @param name the new data store config file name
     */
    public void setDataStoreConfigFileName(String name) {
        mProperties.setProperty(DISKSTORE_CONFIG, name);
        
    }

    /**
     * Gets the data store config file name.
     *
     * @return the data store config file name
     */
    public String getDataStoreConfigFileName() {
        return mProperties.getProperty(DISKSTORE_CONFIG, DEFAULT_UNDEFINED);
    }
    
    /**
     * Sets the histogram enabled.
     *
     * @param b the new histogram enabled
     */
    public void setHistogramEnabled(boolean b)
    {
    	mProperties.setProperty(HISTOGRAM_ENABLED, Boolean.toString(b));
    }
        
    /**
     * Checks if is histogram enabled.
     *
     * @return true, if is histogram enabled
     */
    public boolean isHistogramEnabled(){
    	return Boolean.parseBoolean(mProperties.getProperty(HISTOGRAM_ENABLED, DEFAULT_HISTOGRAM_ENABLED));
    }
    
    /**
     * Sets the histogram bins.
     *
     * @param bins the new histogram bins
     */
    public void setHistogramBins(int bins)
    {
    	mProperties.setProperty(HISTOGRAM_BINS, Integer.toString(bins));
    }
        
    /**
     * Gets the histogram bins.
     *
     * @return the histogram bins
     */
    public int getHistogramBins(){
    	return Integer.parseInt(mProperties.getProperty(HISTOGRAM_BINS, DEFAULT_HISTOGRAM_BINS));
    }
    
    /**
     * Sets the histogram samples.
     *
     * @param samples the new histogram samples
     */
    public void setHistogramSamples(int samples)
    {
    	mProperties.setProperty(HISTOGRAM_SAMPLES, Integer.toString(samples));
    }
        
    /**
     * Gets the histogram samples.
     *
     * @return the histogram samples
     */
    public int getHistogramSamples(){
    	return Integer.parseInt(mProperties.getProperty(HISTOGRAM_SAMPLES, DEFAULT_HISTOGRAM_SAMPLES));
    }
    
    /**
     * Checks if is histogram log enabled.
     *
     * @return true, if is histogram log enabled
     */
    public boolean isHistogramLogEnabled()
    {
    	return Boolean.parseBoolean(mProperties.getProperty(HISTOGRAM_LOG_ENABLED, DEFAULT_HISTOGRAM_LOG_ENABLED));
    }
    
    /**
     * Sets the histogram log enabled.
     *
     * @param value the new histogram log enabled
     */
    public void setHistogramLogEnabled(boolean value)
    {
    	mProperties.setProperty(HISTOGRAM_LOG_ENABLED, Boolean.toString(value));
    }
    
    /**
     * Sets the histogram update interval.
     *
     * @param interval the new histogram update interval
     */
    public void setHistogramUpdateInterval(long interval)
    {
    	mProperties.setProperty(HISTOGRAM_UPDATE_INTERVAL, Long.toString(interval));
    }
        
    /**
     * Gets the histogram update interval.
     *
     * @return the histogram update interval
     */
    public long getHistogramUpdateInterval(){
    	return Long.parseLong(mProperties.getProperty(HISTOGRAM_UPDATE_INTERVAL, DEFAULT_HISTOGRAM_UPDATE_INTERVAL));
    }
    
    /**
     * Gets the lR u2 q insert point.
     *
     * @return the lR u2 q insert point
     */
    public double getLRU2QInsertPoint(){
      return Double.parseDouble(mProperties.getProperty(LRU2Q_INSERT_POINT, DEFAULT_LRU2Q_INSERT_POINT));
    }
    
    /**
     * Sets the lR u2 q insert point.
     *
     * @param d the new lR u2 q insert point
     */
    public void setLRU2QInsertPoint(double d)
    {
      mProperties.setProperty(LRU2Q_INSERT_POINT, Double.toString(d));
    }
    
 
    /**
     * Gets the preeviction list size.
     *
     * @return the preeviction list size
     */
    public int getPreevictionListSize(){
      return Integer.parseInt(mProperties.getProperty(PREEVICTION_LIST_SIZE, DEFAULT_PREEVICTION_LIST_SIZE));
    }
    

    /**
     * Sets the preeviction list size.
     *
     * @param size the new preeviction list size
     */
    public void setPreevictionListSize(int size)
    {
      mProperties.setProperty(PREEVICTION_LIST_SIZE, Integer.toString(size));
    }
    
 
    public int getEvictionAttempts(){
      return Integer.parseInt(mProperties.getProperty(EVICTION_ATTEMPTS, DEFAULT_EVICTION_ATTEMTS));
    }
    
    public void setEvictionAttemts(int attempts)
    {
      mProperties.setProperty(EVICTION_ATTEMPTS, Integer.toString(attempts));
    }
    
    public int getOptimizerThreads()
    {
      return Integer.parseInt(mProperties.getProperty(OPTIMIZER_THREADS, DEFAULT_OPTIMIZER_THREADS));
    }
    
    public void setOptimizerThreads(int threads)
    {
      mProperties.setProperty(OPTIMIZER_THREADS, Integer.toString(threads));      
    }
    
    public int getOptimizerLevel()
    {
      return Integer.parseInt(mProperties.getProperty(OPTIMIZER_LEVEL, DEFAULT_OPTIMIZER_LEVEL));
    }
    
    public void setOptimizerLevel(int level)
    {
      mProperties.setProperty(OPTIMIZER_LEVEL, Integer.toString(level));      
    }
    
    public boolean isOptimizerEnabled()
    {
      return Boolean.parseBoolean(mProperties.getProperty(OPTIMIZER_ENABLED, DEFAULT_OPTIMIZER_ENABLED));
    }
    
    public void setOptimizerEnabled(boolean enabled)
    {
      mProperties.setProperty(OPTIMIZER_ENABLED, Boolean.toString(enabled));      
    }
    
}
