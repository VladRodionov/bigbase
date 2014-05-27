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

import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

// TODO: Auto-generated Javadoc
/** 
 * This thread recycles (reclaim) space using FIFO
 * by evicting data according eviction algorithm in OffHeapCache.
 * It scans storage and purge the oldest file.
 * 
 * TODO:
 * Export statistics (no JMX yet)
 *  
 */
public class FIFOStorageRecycler extends Thread implements StorageRecycler{

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(FIFOStorageRecycler.class);
      
  
  /* Current values */
  
  /** The low watermark. */
  private float lowWatermark;
  
  /** The high watermark. */
  private float highWatermark;
  
  /** The storage this thread works on. */
  private FileExtStorage storage;
    
  /** The total files size. */
  private static AtomicLong totalFilesSize = new AtomicLong(0);
  
  /** The total scanned files. */
  private static AtomicInteger totalScannedFiles = new AtomicInteger(0);
  
  
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
      return FIFOStorageRecycler.totalScannedFiles.get();
    }
    

    /**
     * Gets the total files size.
     *
     * @return the total files size
     */
    public long getTotalFilesSize()
    {
      return FIFOStorageRecycler.totalFilesSize.get();
    }
    
    /**
     * Gets the total purged bytes.
     *
     * @return the total purged bytes
     */
    public long getTotalPurgedBytes()
    {
      return FIFOStorageRecycler.totalPurgedBytes.get();
    }
  }
  
  
  /**
   * Instantiates a new fIFO storage recycler.
   */
  public FIFOStorageRecycler(){ 
    super("FIFO-Recycler");
  }
  
  /**
   * Instantiates a new storage Recycle Thread (GC thread).
   *
   * @param storage the storage
   */

  
  @Override
  public void set(ExtStorage storage){
    this.storage = (FileExtStorage) storage;
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
    dumpConfig();
  }

  /**
   * Dump config.
   */
  private void dumpConfig() {
    LOG.info("Storage Recycler starts with:");
    LOG.info("Low watermark      ="+lowWatermark);
    LOG.info("High watermark     ="+highWatermark);
    
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
        raf.close();
        // STATISTICS
        totalFilesSize.addAndGet(size);
        LOG.info("Processing file: "+fileName+" size="+size);
        // STATISTICS
        totalScannedFiles.incrementAndGet();
        // Update current storage size
        storage.deleteOldestFile();
      } catch (Exception e) {
        LOG.error(fileName, e);
      }
      
    }
    LOG.info(Thread.currentThread().getName()+" stopped.");
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
    
    boolean result = (maxStorageSize * lowWatermark - currentSize) < 0;
    if(result) return true;
    long partitionSize = storage.getTotalPartitionSize();
    if(partitionSize != 0L){
    	long usableSize = storage.getUsablePartitionSpace();    	
    	result = partitionSize * ( 1-lowWatermark) > usableSize; 
    	if(result){
    		LOG.warn("Usable partition size is low: "+usableSize +" of total "+partitionSize);
    	}
    } 
	return result;

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
   * Gets the storage.
   *
   * @return the storage
   */
  public ExtStorage getStorage()
  {
    return storage;
  }
}


  

