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
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.common.util.NumericHistogram;
import com.koda.integ.hbase.util.FileUtils;

// TODO: Auto-generated Javadoc
/** 
 * This thread recycles (reclaim) space
 * by evicting data according eviction algorithm in OffHeapCache.
 * It scans storage the oldest storage file object by object and purges objects
 * eligible for eviction, all other objects are rewritten into the current open storage file.
 * 
 * 
 * There are 3 parameters which defines Recycler behavior:
 * 1. Maximum Read Throughput  (comes from Configuration). This is disk IO throttling
 *    mechanism. This limit is not hard. It can be dynamically increased if 
 *    StorageRecycler can not keep up with increasing storage size due to
 *    low cache hit rate.
 * 2. EvictionPercentile eligible threshold (EPT). Explain on LRU example:
 *    OffHeapCache tracks approximate distribution of object LRU times (Eviction Value)
 *    This information is available for StorageRecycler (SR). When SR  
 *    scans storage file it makes decision whether to keep the object or not based on  EPT
 *    For example: if EPT = 0.5 - it means that ALL objects which fall into bottom 50% (0.5) of 
 *    object EV distribution histogram are eligible for deletion. 
 *    EPT is not a hard limit as well and can be dynamically adjusted based on  a current 
 *    storage load.
 * 3. Write Batch Size. Default is 2. If object is still usable it needs to be stored in a
 *    current open storage file (the most recent one). By varying this parameter we can vary 
 *    the Recycler throughput.  
 *    
 * TODO:
 * + 1. ioThrottle test
 * 2. Dynamic adjustment of a recycle's parameters
 * + 3. Export statistics (no JMX yet)
 *  
 */
public class LRUStorageRecycler extends Thread implements StorageRecycler{

	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(LRUStorageRecycler.class);
	

	
	/** Maximum read throughput in MB per second. */
	public final static String MAX_READ_THROUGHPUT_CONF   = "file.recycler.max.read.throughput";
	
	/** Eviction threshold. */
	public final static String EVICTION_THRESHOLD_CONF    = "file.recycler.eviction.threshold";
	
	/** Active object write batch size. */
	public final static String WRITE_BATCH_SIZE_CONF      = "file.recycler.write.batch.size";
	
	/** Adaptive algorithm enabled?. */
	public final static String ADAPTIVE_ALGO_ENABLED_CONF     = "file.recycler.adaptive.algo.enabled";
		
	
	/* Default values */
	/** Storage low watermark. */	
	public final static String STORAGE_RATIO_LOW_DEFAULT     = "0.9f";	
	
	/** Storage high watermark. */	
	public final static String STORAGE_RATIO_HIGH_DEFAULT    = "0.95f";	
	
	/** Maximum read throughput in MB per second. */
	public final static String MAX_READ_THROUGHPUT_DEFAULT   = "10"; // 10MB per sec
	/** Eviction threshold default is 0.25*/
	public final static String EVICTION_THRESHOLD_DEFAULT    = "0.25";
	
	/** Active object write batch size. */
	public final static String WRITE_BATCH_SIZE_DEFAULT      = "2";
	
	/** Adaptive algorithm enabled?. */
	public final static String ADAPTIVE_ALGO_ENABLED_DEFAULT     = "false";
		
	
	/* Current values */
	
	/** The low watermark. */
	private float lowWatermark;
	
	/** The high watermark. */
	private float highWatermark;
	
	/** Panic level watermark. */
	private float panicLevelWatermark = 0.99f;
	
	/** The max read throughput in MBs. */
	private int maxReadThrougput ;
	
	/** The eviction threshold. */
	private float evictionThreshold;
	
	/** The write batch size. */
	private int writeBatchSize;
	
	/** The adaptive algorithm enabled. */
	private boolean adaptiveAlgoEnabled;
		
	/** The in-memory reference cache. This LRU cache contains references to external (file) storage */
	private OffHeapCache refCache;

	/** The storage this thread works on. */
	private FileExtStorage storage;
	
	/** Recycle thread statistics. */
	private static AtomicLong totalScannedBytes = new AtomicLong(0);
	
	/** The total files size. */
	private static AtomicLong totalFilesSize = new AtomicLong(0);
	
	/** The total scanned files. */
	private static AtomicInteger totalScannedFiles = new AtomicInteger(0);
	
	/** The total panic events. */
	private static AtomicInteger totalPanicEvents = new AtomicInteger(0);
	
	/** The total purged bytes. */
	private static AtomicLong totalPurgedBytes = new AtomicLong(0);
	
	
	/**
	 * The Class Statistics.
	 */
	public class Statistics
	{
		
		/**
		 * Gets the total scanned files.
		 *
		 * @return the total scanned files
		 */
		public int getTotalScannedFiles(){
			return LRUStorageRecycler.totalScannedFiles.get();
		}
		
		/**
		 * Gets the total panic events.
		 *
		 * @return the total panic events
		 */
		public int getTotalPanicEvents()
		{
			return LRUStorageRecycler.totalPanicEvents.get();			
		}
		
		/**
		 * Gets the total scanned bytes.
		 *
		 * @return the total scanned bytes
		 */
		public long getTotalScannedBytes(){
			return LRUStorageRecycler.totalScannedBytes.get();
		}
		
		/**
		 * Gets the total files size.
		 *
		 * @return the total files size
		 */
		public long getTotalFilesSize()
		{
			return LRUStorageRecycler.totalFilesSize.get();
		}
		
		/**
		 * Gets the total purged bytes.
		 *
		 * @return the total purged bytes
		 */
		public long getTotalPurgedBytes()
		{
			return LRUStorageRecycler.totalPurgedBytes.get();
		}
	}
	
  /**
   * Instantiates a new storage Recycle Thread (GC thread).
   *
   */	
	public LRUStorageRecycler(){
	  super("LRU-Recycler");
	}


	
	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.StorageRecycler#set(com.koda.integ.hbase.storage.ExtStorage)
	 */
	@Override
	public void set(ExtStorage storage){
	  this.storage = (FileExtStorage) storage;
	  this.refCache = this.storage.getStorageRefCache();
	  init(this.storage.getConfig());
	}
	
	
	/**
	 * Gets the statistics.
	 *
	 * @return the statistics
	 */
	public Statistics getStatistics()
	{
		return new Statistics();
	}
	
	/**
	 * Inits the.
	 *
	 * @param config the config
	 */
	private void init(Configuration config) {
		
		lowWatermark = Float.parseFloat(config.get(STORAGE_RATIO_LOW_CONF, STORAGE_RATIO_LOW_DEFAULT));
		highWatermark = Float.parseFloat(config.get(STORAGE_RATIO_HIGH_CONF, STORAGE_RATIO_HIGH_DEFAULT));
		maxReadThrougput = Integer.parseInt(config.get(MAX_READ_THROUGHPUT_CONF, MAX_READ_THROUGHPUT_DEFAULT));
		evictionThreshold = Float.parseFloat(config.get(EVICTION_THRESHOLD_CONF, EVICTION_THRESHOLD_DEFAULT));
		writeBatchSize = Integer.parseInt(config.get(WRITE_BATCH_SIZE_CONF, WRITE_BATCH_SIZE_DEFAULT));
		adaptiveAlgoEnabled = Boolean.parseBoolean(config.get(ADAPTIVE_ALGO_ENABLED_CONF, ADAPTIVE_ALGO_ENABLED_DEFAULT));
		dumpConfig();
	}

	/**
	 * Dump config.
	 */
	private void dumpConfig() {
		LOG.info("Storage Recycler starts with:");
		LOG.info("Low watermark      ="+lowWatermark);
		LOG.info("High watermark     ="+highWatermark);
		LOG.info("Max read tput      ="+maxReadThrougput+"MBs");
		LOG.info("Eviction threshold ="+evictionThreshold);
		LOG.info("Write batch size   ="+writeBatchSize);
		LOG.info("Adaptive algorithm ="+adaptiveAlgoEnabled);
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run(){
		LOG.info(Thread.currentThread().getName()+" started.");
		
		while(needContinue()){
			// Get oldest file			
			int minId = storage.getMinId().get();
			String fileName = storage.getFilePath(minId);
			RandomAccessFile raf = storage.getFile(minId);
			
			try {
				long size = raf.length();
				// STATISTICS
				totalFilesSize.addAndGet(size);
				LOG.info("Processing file: "+fileName+" size="+size);
				// Its FIFO now
				processFile(raf);
				// STATISTICS
				totalScannedFiles.incrementAndGet();
				// Update current storage size
				storage.updateStorageSize(-size);
				adjustRecyclerParameters();

			} catch (Exception e) {
				LOG.error(fileName, e);
			}
			
		}
		LOG.info(Thread.currentThread().getName()+" stopped.");
	}

	/**
	 * Adjust recycler parameters.
	 *
	 */
	private void adjustRecyclerParameters() {
		if(adaptiveAlgoEnabled == false) return ;		
		//TODO
	}

	/**
	 * Format of a block in a file:
	 * 0..3  - total record size (-4)
	 * 4..7  - size of a key in bytes (16 if use hash128)
	 * 8 .. x - key data
	 * x+1 ..x+1- IN_MEMORY flag ( 1- in memory, 0 - not)
	 * x+2 ... block, serialized and compressed
	 *
	 * @param file the file
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws NativeMemoryException the native memory exception
	 */
	private void processFile(RandomAccessFile file) throws IOException, NativeMemoryException {

		FileChannel fc = file.getChannel();	
		// make sure that file size < 2G
		LOG.info("File length="+ file.length());
		MappedByteBuffer buffer = fc.map(MapMode.READ_ONLY, 0, file.length());

		long fileLength = file.length();
		long saved = 0;
		long startTime = System.currentTimeMillis();
		
		while (buffer.position() < fileLength){
			int oldOffset = buffer.position();
			//LOG.info(oldOffset);
			// check IO throttle
			ioThrottle(startTime, oldOffset);
			
			NumericHistogram histogram = refCache.getObjectHistogram();
			
			int blockSize = buffer.getInt();
			int keySize =   buffer.getInt();
			
			//LOG.info("block size="+blockSize+" key size="+keySize);
			
			byte[] key = new byte[keySize];
			// STATISTICS
			totalScannedBytes.addAndGet(blockSize + 4);
			
			// read key
			// WE HAVE TO USE byte[] keys
			long data = refCache.getEvictionData(key);
			if( data < 0){
				// not found in in_memory cache
			  buffer.position(oldOffset + blockSize + 4);
				continue;
			}
			
			double quantValue = histogram.quantile(evictionThreshold);
			if(data > quantValue){
				// save block
				saved = blockSize + 4;
				buffer.position(oldOffset);
				StorageHandle handle = storage.storeData(buffer);
				refCache.put(key, handle);

			} else{
				// STATISTICS
				totalPurgedBytes.addAndGet(blockSize + 4);
			}
			
			if(oldOffset + blockSize + 4 < fileLength){
				// Advance pointer
				buffer.position(oldOffset + blockSize + 4);

			} else{
				break;
			}
			// Check panic. W/o adaptive processing support - killing file entirely 
			// is the only option to keep up with the load.
			if( storage.getCurrentStorageSize() >= panicLevelWatermark * storage.getMaxStorageSize()){
				LOG.warn("[PANIC DELETE]. Storage size exceeded "+panicLevelWatermark+" mark.");
				// STATISTICS
				totalPanicEvents.incrementAndGet();
			}			
		}
		
		// Unmap mapped ByteBuffer
		fc.close();
		FileUtils.unmapMmaped(buffer);
;
		LOG.info("Stats: total length="+fileLength+"; purged data="+
				(fileLength - saved)+" with eviction threshold="+evictionThreshold +
				"; purged ratio=["+(((double)(fileLength - saved))/fileLength)+"]");
		
	}
	

	
	/**
	 * Io throttle.
	 *
	 * @param startTime the start time
	 * @param oldOffset the old offset
	 */
	private void ioThrottle(long startTime, int oldOffset) {
		
		long time = System.currentTimeMillis();
		//if(time - startTime < 10) return; // do not do any throttling first 10ms
		long expectedSize = (long)(((double)(time - startTime)/ 1000) * maxReadThrougput * 1000000);
		if(oldOffset > expectedSize){
			long sleep = (oldOffset - expectedSize) / (maxReadThrougput * 1000);
			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
			}
		}
		
	}

	/**
	 * Need continue.
	 *
	 * @return true, if successful
	 */
	private boolean needContinue()
	{
		long maxStorageSize = storage.getMaxStorageSize();
		long currentSize = storage.getCurrentStorageSize();		
		return (maxStorageSize * lowWatermark - currentSize) < 0;
	}
	
	/**
	 * Gets the low watermark.
	 *
	 * @return the lowWatermark
	 */
	public float getLowWatermark() {
		return lowWatermark;
	}

	/**
	 * Sets the low watermark.
	 *
	 * @param lowWatermark the lowWatermark to set
	 */
	public void setLowWatermark(float lowWatermark) {
		this.lowWatermark = lowWatermark;
	}

	/**
	 * Gets the high watermark.
	 *
	 * @return the highWatermark
	 */
	public float getHighWatermark() {
		return highWatermark;
	}

	/**
	 * Sets the high watermark.
	 *
	 * @param highWatermark the highWatermark to set
	 */
	public void setHighWatermark(float highWatermark) {
		this.highWatermark = highWatermark;
	}

	/**
	 * Gets the max read througput.
	 *
	 * @return the maxReadThrougput
	 */
	public int getMaxReadThrougput() {
		return maxReadThrougput;
	}

	/**
	 * Sets the max read througput.
	 *
	 * @param maxReadThrougput the maxReadThrougput to set
	 */
	public void setMaxReadThrougput(int maxReadThrougput) {
		this.maxReadThrougput = maxReadThrougput;
	}

	/**
	 * Gets the eviction threshold.
	 *
	 * @return the evictionThreshold
	 */
	public float getEvictionThreshold() {
		return evictionThreshold;
	}

	/**
	 * Sets the eviction threshold.
	 *
	 * @param evictionThreshold the evictionThreshold to set
	 */
	public void setEvictionThreshold(float evictionThreshold) {
		this.evictionThreshold = evictionThreshold;
	}

	/**
	 * Gets the write batch size.
	 *
	 * @return the writeBatchSize
	 */
	public int getWriteBatchSize() {
		return writeBatchSize;
	}

	/**
	 * Sets the write batch size.
	 *
	 * @param writeBatchSize the writeBatchSize to set
	 */
	public void setWriteBatchSize(int writeBatchSize) {
		this.writeBatchSize = writeBatchSize;
	}

	/**
	 * Checks if is adaptive algo enabled.
	 *
	 * @return the adaptiveAlgoEnabled
	 */
	public boolean isAdaptiveAlgoEnabled() {
		return adaptiveAlgoEnabled;
	}

	/**
	 * Sets the adaptive algo enabled.
	 *
	 * @param adaptiveAlgoEnabled the adaptiveAlgoEnabled to set
	 */
	public void setAdaptiveAlgoEnabled(boolean adaptiveAlgoEnabled) {
		this.adaptiveAlgoEnabled = adaptiveAlgoEnabled;
	}
	
	/**
	 * Gets the storage.
	 *
	 * @return the storage
	 */
	public ExtStorage getStorage()
	{
		return storage;
	}
}

class G1Algo implements StorageRecyclerAlgo{

	
	
	@Override
	public void adjustRecyclerParameters() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init(StorageRecycler storage) {
		// TODO Auto-generated method stub
		
	}
	
}
