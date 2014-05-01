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
package com.inclouds.hbase.rowcache;

import java.io.File;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.Filter.ReturnCode;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

import sun.misc.Unsafe;

import com.inclouds.hbase.utils.ConfigHelper;
import com.inclouds.hbase.utils.Hints;
import com.inclouds.hbase.utils.RequestContext;
import com.koda.KodaException;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;
import com.koda.persistence.PersistenceMode;
import com.koda.persistence.ProgressListener;
import com.koda.persistence.rawfs.RawFSConfiguration;
import com.koda.persistence.rawfs.RawFSStore;

// TODO: Auto-generated Javadoc
/**
 * 
 * This is simplified version of ScanCache. 
 * 
 * 1) We always cache data on Get by CF (the entire column family is cached)
 * 2) We invalidate cached CF on every Put/Delete/Append/Increment operation if it involve cached CF
 *
 * TODO: There is one issue unresolved:
 * 
 *   1. Delete table or Delete column family MUST invalidate all the cache for the table
 *   
 *     Check preClose/preOpen, postClose/postOpen. I think when we disable/enable table all regions are closed/opened
 *     Yep, this is what we should do ...
 *     
 *     
 *  1. On preClose we add table (if it ROWCACHE) to the list of closed and record as well ALL its column families
 *  
 *  2. On preGet we need to check table if it is closed/disabled
 *  
 *  3. On preOpen we check table and ALL its families and compare them with those from the list of closed. If we detect
 *     deleted family (ROWCACHE) than we need to:
 *     
 *  A. temp disable cache and delete all records from cache for this CF   
 * 
 * 
 * Some ideas:
 * 
 * 1. Region HA - have two RS (master and slave) to serve the same region - HA
 * 2. Mitigate block cache purge on compaction                            - CAP (continuously available performance) 
 * 3. Cell level security and access control (from Accumulo)              - Security
 * 4. Minimize MTTR (faster failure detection + Region HA). Goal - sub-second  MTTR 
 * 5. Compaction storms?   Avoid network transfers (requires HDFS support)
 * 6. Region splits. Rolling splits?   
 * 7. Improve block locality (how - ask FB)
 * 8. BlockCache persistence SSD (use FIFO eviction)        
 * 
 * 9. Flushing & compactions (Acumulo compaction algorithm)
 * 
 * Thanks for the clarification. I also came across a previous thread which
 sort of talks about a similar problem.
 http://mail-archives.apache.org/mod_mbox/hbase-user/201204.mbox/%3CCAGpTDNfWNRsNqV7n3wgjE-iCHZPx-CXn1TBchgwRPOhgcoS+bw@mail.gmail.com%3E

 I guess my problem is also similar to the fact that my writes are well
 distributed and at a given time I could be writing to a lot of regions.
 Some of the regions receive very little data but since the flush algorithm
 choose at random what to flush when "too many hlogs" is hit, it will flush
 a region with less than 10mb of data causing too many small files. This
 in-turn causes compaction storms where even though major compactions is
 disabled, some of the minor get upgraded to major and that's when things
 start getting worse.

 My compaction queues are still the same and so I doubt I will be coming out
 of this storm without bumping up max hlogs for now. Reducing regions per
 server is one option but then I will be wasting my resources since the
 servers at current load are at < 30% CPU and < 25% RAM. Maybe I can bump up
 heap space and give more memory to the the memstore. Sorry, I am just
 thinking out loud.    


 * @author vrodionov
 *
 */

/**
 * TODO 
 * 1. Invalidate cache or do something on bulk load!!!
 * + 2. ScanCache configuration per CF (Done, but we need tool to set ROWCACHE per table/cf)
 * 3. store on shutdown, load on start up, periodic snapshots (LARGE)
 * 4. metrics, JMX - HBase integration (LARGE) 
 * 5. Smarter cache invalidation on a per table:family basis (MEDIUM)
 * 6. Cache configuration - ROWCACHE On/Off must be integrated into HBase console. (HBase patch) (MEDIUM)
 * 7. Separate tool to enable/disable ROWCACHE per table:family. (SMALL-MEDIUM)
 * 8. Row+filter cache. Filter must support ID (byte[] array). ???
 * 
 */

/**
 * You could always set hbase.online.schema.update.enable to true on your
 * master, restart it (but not the cluster), and you could do what you are
 * describing... but it's a risky feature to use before 0.96.0.
 * 
 * Did you also set hbase.replication to true? If not, you'll have to do it on
 * the region servers and the master via a rolling restart.
 */
public class RowCache {

  public final static String ROWCACHE_MAXMEMORY = "offheap.rowcache.maxmemory";

  public final static String ROWCACHE_MAXITEMS = "offheap.rowcache.maxitems";

  // NONE (default), LZ4, SNAPPY
  public final static String ROWCACHE_COMPRESSION = "offheap.rowcache.compression";

  public final static String ROWCACHE_BUFFER_SIZE = "offheap.rowcache.nativebuffer.size";

  public final static String ROWCACHE_PERSISTENT = "offheap.rowcache.persistent";
  
  public final static String ROWCACHE_CACHE_DATA_ROOTS = "offheap.rowcache.storage.dir";

  public final static String DEFAULT_PERSISTENT = "false";
  
  

  public final static long DEFAULT_MAX_MEMORY = 1000000000L; // 1G by default

  public final static int DEFAULT_MAXITEMS = 10000000; // 10M

  public final static String DEFAULT_COMPRESSION = "NONE";

  // Default buffer size is 256K (It does not make sense to cache rows larger
  // than 256K anyway )
  public final static int DEFAULT_BUFFER_SIZE = 256 * 1024;

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(RowCache.class);

  /** The byte buffer thread - local storage. */
  private static ThreadLocal<ByteBuffer> bufTLS = new ThreadLocal<ByteBuffer>();

  private static int ioBufferSize = DEFAULT_BUFFER_SIZE;

  /**
   * The Row Cache.
   * 
   * The single instance per region server
   * 
   * */

  private static OffHeapCache rowCache;

  private static Thread statThread;

  private static long lastRequests, lastHits;

  private static int STATS_INTERVAL = 30000;

  /** The instance. */
  public static RowCache instance;

  /** The disabled. */
  private static volatile AtomicBoolean disabled = new AtomicBoolean(false);

  /** Query (GET) context (thread local). */
  private static ThreadLocal<RequestContext> contextTLS = new ThreadLocal<RequestContext>() {

    @Override
    protected RequestContext initialValue() {
      return new RequestContext();
    }

  };

  /**
   * List of pending tables in close state We need to persist it to be able to
   * handle serious failures TODO.
   **/
  // private static TreeMap<byte[], List<byte[]>> pendingClose =
  // new TreeMap<byte[], List<byte[]>>(Bytes.BYTES_COMPARATOR);

  /** The mutations in progress. Current number of a cache mutate operations */
  private static AtomicLong mutationsInProgress = new AtomicLong(0);

  private static boolean isPersistentCache = false;

  /**
   * The families TTL map. This is optimization, as since standard HBase API to
   * get CF's TTL is not very efficient We update this map on preOpen.
   * 
   **/
  private static TreeMap<byte[], Integer> familyTTLMap = new TreeMap<byte[], Integer>(
      Bytes.BYTES_COMPARATOR);

  private Configuration config;

  public static void reset() {
    instance = null;
    rowCache = null;
  }

  /**
   * Sets the disabled.
   * 
   * @param b
   *          the new disabled
   * @return true, if successful
   */
  public boolean setDisabled(boolean b) {
    return disabled.compareAndSet(!b, b);
  }

  /**
   * Checks if is disabled.
   * 
   * @return true, if is disabled
   */
  public boolean isDisabled() {
    return disabled.get();
  }

  /** The trace. */
  private boolean trace = false;

  /**
   * Sets the trace.
   * 
   * @param v
   *          the new trace
   */
  public void setTrace(boolean v) {
    trace = v;
  }

  /**
   * Checks if is trace.
   * 
   * @return true, if is trace
   */
  public boolean isTrace() {
    return trace;
  }

  /**
   * Start co-processor - cache.
   * 
   * @param cfg
   *          the cfg
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void start(Configuration cfg) throws IOException {

    // Get all config from Configuration object
    // Start - load cache

    this.config = cfg;

    synchronized (RowCache.class) {

      if (rowCache != null)
        return;

      final CacheConfiguration ccfg = ConfigHelper.getCacheConfiguration(cfg);
      // set cache name
      ccfg.setCacheName("row-cache");
      
      long maxMemory = cfg.getLong(ROWCACHE_MAXMEMORY, DEFAULT_MAX_MEMORY);
      ccfg.setMaxMemory(maxMemory);
      LOG.info("[row-cache] Setting max memory to " + maxMemory);
      long maxItems = cfg.getLong(ROWCACHE_MAXITEMS, DEFAULT_MAXITEMS);
      if (maxItems > Integer.MAX_VALUE - 1) {
        maxItems = Integer.MAX_VALUE - 1;
        LOG.warn("[row-cache] Max items is too large " + maxItems);
      } else {
        LOG.info("[row-cache] Setting max items to " + maxItems);
      }

      LOG.info("[row-cache] Direct memory buffer size set to "
          + StringUtils.byteDesc(RowCache.ioBufferSize));

      ccfg.setBucketNumber((int) maxItems);
      String codecName = cfg.get(ROWCACHE_COMPRESSION, DEFAULT_COMPRESSION);

      ccfg.setCodecType(CodecType.valueOf(codecName.toUpperCase()));
      LOG.info("[row-cache] compression codec=" + codecName);
      
      isPersistentCache = Boolean.parseBoolean(cfg.get(ROWCACHE_PERSISTENT, DEFAULT_PERSISTENT));
      
      LOG.info("[row-cache] persistent="+isPersistentCache);
      
      String[] dataRoots = getDataRoots(cfg.get(ROWCACHE_CACHE_DATA_ROOTS));
      
      if(isPersistentCache && dataRoots == null){
        dataRoots = getHDFSRoots(cfg);
        
        if(dataRoots == null){
          LOG.warn("Data roots are not defined for Row Cache. Set persistent mode to false.");
          isPersistentCache = false;
        }
      }
      // TODO - compression
      CacheManager manager = CacheManager.getInstance();
      try{
      
        if (isPersistentCache) {
          RawFSConfiguration storeConfig = new RawFSConfiguration();
          storeConfig.setDiskStoreImplementation(RawFSStore.class);
          storeConfig.setStoreName(ccfg.getCacheName());
          storeConfig.setDbDataStoreRoots(dataRoots);
          storeConfig.setPersistenceMode(PersistenceMode.ONDEMAND);
          storeConfig.setDbSnapshotInterval(15);
          ccfg.setDataStoreConfiguration(storeConfig);
          // Load cache data
          rowCache = manager.getCache(ccfg, null);
        } else {

          rowCache = manager.getCache(ccfg, new ProgressListener() {

            @Override
            public void canceled() {
              LOG.info("Canceled");
            }

            @Override
            public void error(Throwable t, boolean aborted) {
              LOG.error("Aborted=" + aborted, t);
            }

            @Override
            public void finished() {
              LOG.info("Finished loading cache");
            }

            @Override
            public void progress(long done, long total) {
              LOG.info("Loaded " + done + " out of " + total);
            }

            @Override
            public void started() {
              LOG.info("Started loading scan cache data from "
                  + ccfg.getDiskStoreConfiguration().getDbDataStoreRoots());
            }
          });

        }
      } catch (Throwable ex) {
        throw new IOException(ex);
      }

      LOG.info("[row-cache] coprocessor started ");
      
      RowCache.instance = this;

      Runnable r = new Runnable() {
        public void run() {
          LOG.info("[row-cache] Stats thread started. ");
          while (true) {
            try {
              Thread.sleep(STATS_INTERVAL);
            } catch (InterruptedException e) {
            }

            long lastR = lastRequests;
            long lastH = lastHits;
            long requests = rowCache.getTotalRequestCount();
            long hits = rowCache.getHitCount();
            if (requests != lastRequests) {
              // Log only if new data
              LOG.info("Row cache stats: accesses="
                  + requests
                  + " hits="
                  + hits
                  + " hitRatio="
                  + ((requests == 0) ? "0.00" : StringUtils.formatPercent(
                      (double) hits / requests, 2)
                      + "%"
                      + " Last period: accesses="
                      + (requests - lastR)
                      + " hits="
                      + (hits - lastH)
                      + " hitRatio="
                      + (((requests - lastR) == 0) ? "0.00" : StringUtils
                          .formatPercent((double) (hits - lastH)
                              / (requests - lastR), 2)))
                  + "%"
                  + " maxMemory="
                  + StringUtils.byteDesc(rowCache.getMemoryLimit())
                  + " allocatedMemory="
                  + StringUtils.byteDesc(rowCache.getAllocatedMemorySize())
                  + " freeMemory="
                  + StringUtils.byteDesc(rowCache.getMemoryLimit()
                      - rowCache.getAllocatedMemorySize()) + " totalItems="
                  + rowCache.size() + " evicted=" + rowCache.getEvictedCount());
              lastRequests = requests;
              lastHits = hits;
            }
          }
        }
      };

      statThread = new Thread(r, "BigBaseRowCache.StatisticsThread");
      statThread.start();
      // Register shutdown hook
      registerShutdownHook();
    }
  }

  private void registerShutdownHook() {

    if(isPersistentCache == false) return;
    Runtime.getRuntime().addShutdownHook( new Thread(){
      public void run(){
        LOG.info("Shutting down row-cache, saving data started ...");
        long startTime = System.currentTimeMillis();
        long totalRows = rowCache.size();
        long totalSize = rowCache.getAllocatedMemorySize();
        try {
          rowCache.save();
        } catch (KodaException e) {
          LOG.error("Failed to save row-cache", e);
          return;
        }
        LOG.info("Saved "+StringUtils.byteDesc(totalSize)+" Total Rows:"+totalRows+" in "+
           (System.currentTimeMillis() - startTime)+" ms" );
        
      }
    });
    
    LOG.info("[row-cache] Registered shutdown hook");
    
  }
  

  private String[] getHDFSRoots(Configuration conf) {
    // Use default dfs data directories
    String str = conf.get("dfs.data.dir");
    if(str == null) return null;
    String[] dirs = str.split(",");
    for(int i=0 ; i < dirs.length; i++){
      dirs[i] = dirs[i].trim() + File.separator + "blockcache";
    }
    return dirs;
  }

  private String[] getDataRoots(String roots)
  {
    if (roots == null) return null;
    String[] rts = roots.split(",");
    String[] retValue = new String[rts.length];
    for( int i=0; i < retValue.length; i++){
      retValue[i] = rts[i].trim();
    }
    return retValue;
  }
  
  private void checkLocalBufferAllocation() {
    if (bufTLS.get() != null)
      return;
    RowCache.ioBufferSize = config.getInt(RowCache.ROWCACHE_BUFFER_SIZE,
        RowCache.DEFAULT_BUFFER_SIZE);
    bufTLS.set(NativeMemory.allocateDirectBuffer(16, RowCache.ioBufferSize));
  }

  private final ByteBuffer getLocalByteBuffer() {
    checkLocalBufferAllocation();
    return bufTLS.get();
  }

  /**
   * Stop.
   * 
   * @param e
   *          the e
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void stop(CoprocessorEnvironment e) throws IOException {
    // TODO
    LOG.info("[row-cache] Stopping row-cache started ...");
    LOG.info("[row-cache] Stopping row-cache finished.");
  }

  /**
   * DISABLED Pre - close. This method is called when :
   * 
   * 1. Table is deleted 2. Table is modified 3. In some other cases
   * 
   * What: abortRequested?
   * 
   * @param desc
   *          the desc
   * @param abortRequested
   *          the abort requested
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  // public void preClose(HTableDescriptor desc, boolean abortRequested)
  // throws IOException
  // {
  // byte[] tableName = desc.getName();
  //		
  // synchronized (pendingClose) {
  //			
  // if(isTrace()){
  // LOG.error("[row-cache] preClose: table="+new
  // String(tableName)+" abortRequested="+abortRequested);
  // }
  // if (pendingClose.containsKey(tableName)){
  //				
  // if(isTrace()) LOG.error("[row-cache] preClose: contains "+ new
  // String(tableName));
  // return;
  // }
  // HColumnDescriptor[] columns = desc.getColumnFamilies();
  // List<byte[]> colList = new ArrayList<byte[]>();
  // for (HColumnDescriptor cd : columns) {
  // if (isRowCacheEnabledForFamily(desc, cd)) {
  // colList.add(cd.getName());
  // } else{
  // if(isTrace()){
  // LOG.error("not cached: "+ new String(cd.getName()));
  // }
  // }
  // }
  // if (colList.size() > 0) {
  // // Add table to the pending close list
  // pendingClose.put(tableName, colList);
  // }
  // if(isTrace()){
  // LOG.error("[row-cache] preClose: families:");
  // for(byte[] f: colList){
  // LOG.error(new String(f) +" - pending");
  // }
  // }
  // }
  // if(isTrace()){
  // LOG.error("[row-cache] preClose: table="+new String(tableName)+" - done.");
  // }
  // }
  //  	
  // public void postClose(HTableDescriptor desc, boolean abortRequested)
  // throws IOException
  // {
  // byte[] tableName = desc.getName();
  // LOG.error("[row-cache] postClose: table="+new
  // String(tableName)+" abortRequested="+abortRequested);
  //	
  // }

  /**
   * Pre open. Its a blocking call
   * 
   * @param desc
   *          the desc
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  // public void preOpen(HTableDescriptor desc)
  // throws IOException
  // {
  //		
  // synchronized (pendingClose) {
  // if(isTrace()){
  // LOG.error("[row-cache] preOpen: "+new String(desc.getName()));
  // }
  // byte[] tableName = desc.getName();
  // List<byte[]> families = pendingClose.get(tableName);
  // if (families == null) {
  // // We open new table
  // return;
  // }
  // if(isTrace()){
  // LOG.error("[row-cache] preOpen: pending CF list:");
  // for(byte[] f: families){
  // LOG.error(new String(f) +" - ROWCACHE");
  // }
  // }
  // HColumnDescriptor[] coldesc = desc.getColumnFamilies();
  // List<byte[]> famList = new ArrayList<byte[]>();
  // for (int i = 0; i < coldesc.length; i++) {
  // if(isRowCacheEnabledForFamily(desc, coldesc[i])){
  // famList.add(coldesc[i].getName());
  // }
  // }
  // Collections.sort(famList, Bytes.BYTES_COMPARATOR);
  // if(isTrace()){
  // LOG.error("[row-cache] preOpen: new cached CF list:");
  // for(byte[] f: famList){
  // LOG.error(new String(f) +" - ROWCACHE");
  // }
  // }
  // // TODO
  // List<byte[]> result = ListUtils.minus(families, famList,
  // Bytes.BYTES_COMPARATOR, new ArrayList<byte[]>());
  // if(isTrace()){
  // LOG.error("[row-cache] preOpen: retired CF list:");
  // for(byte[] f: result){
  // LOG.error(new String(f) +" - retired");
  // }
  // }
  // // TODO - update familyTTLMap
  // for (HColumnDescriptor d : coldesc) {
  // int ttl = d.getTimeToLive();
  // byte[] key = Bytes.add(tableName, d.getName());
  // familyTTLMap.put(key, ttl);
  //
  // }
  // pendingClose.remove(tableName);
  //
  // if (result.size() > 0 && isDisabled() == false) {
  // // Run this in a separate thread
  // if(setDisabled(true) == false) return ;
  // clearCache();
  // }
  // }
  //		
  // }

  /**
   * Just updates family TTL map
   */
  public void preOpen(HTableDescriptor desc) throws IOException {

    if (isTrace()) {
      LOG.info("[row-cache] preOpen: " + new String(desc.getName()));
    }

    synchronized (familyTTLMap) {
      byte[] tableName = desc.getName();
      HColumnDescriptor[] coldesc = desc.getColumnFamilies();
      for (HColumnDescriptor d : coldesc) {
        int ttl = d.getTimeToLive();
        byte[] key = Bytes.add(tableName, d.getName());
        familyTTLMap.put(key, ttl);

      }
    }

  }

  /**
   * Pre - bulkLoad HFile. Bulk load for CF with rowcache enabled is not a good
   * practice and should be avoided as since we clear all cache entries for this
   * CF (in a future) Currently, bulk load operation which involves at least one
   * cachable CF will result in an entire cached data loss.
   * 
   * @param tableDesc
   *          the table desc
   * @param familyPaths
   *          the family paths
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void preBulkLoadHFile(HTableDescriptor tableDesc,
      List<Pair<byte[], String>> familyPaths) throws IOException {
    // we need to keep list of tables to cache and if this
    // TODO - OPTIMIZE!!!
    // This MUST be blocking operation
    // Disable cache for read operations only
    if (isDisabled()) {
      LOG.info("[row-cache][preBulkLoadHFile] Cache disabled, skip operation.");
      return;
    }

    List<byte[]> families = new ArrayList<byte[]>();
    for (Pair<byte[], String> p : familyPaths) {
      families.add(p.getFirst());
    }

    if (isRowCacheEnabledForFamilies(tableDesc, families) == false) {
      LOG.info("[row-cache][preBulkLoadHFile] skipped. No families cached.");
      return;
    }

    if (setDisabled(true) == false)
      return;
    // Run cache cleaning in a separate thread
    clearCache();

  }

  /**
   * Clear cache.
   */
  private void clearCache() {
    // TODO we can not clear cache w/o proper locking
    // as since we can not block delete operations on cache
    // during cache cleaning , but we can do this if we purge data entirely

    Runnable r = new Runnable() {
      public void run() {

        // Check if cache is disabled already, bail out if - yes
        // if(setDisabled(true) == false) return ;
        // Wait for all mutation operations to finish
        waitMutationsZero();

        try {
          rowCache.clear();
        } catch (NativeMemoryException e) {
          LOG.error("[row-cache] Failed to clear row-cache", e);
        } finally {
          setDisabled(false);
        }
      }
    };

    Thread t = new Thread(r);
    t.start();
  }

  /**
   * Wait mutations zero.
   */
  protected static void waitMutationsZero() {

    while (mutationsInProgress.get() != 0) {
      try {
        Thread.sleep(1);// Sleep 1 ms
      } catch (Exception e) {
      }
    }

  }

  /**
   * CHECKED 2 Append operation works only for fully qualified columns (with
   * versions).
   * 
   * @param tableDesc
   *          the table desc
   * @param append
   *          the append
   * @return the result
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public Result preAppend(HTableDescriptor tableDesc, Append append)
      throws IOException {
    if (isDisabled())
      return null;
    try {
      mutationsInProgress.incrementAndGet();
      byte[] tableName = tableDesc.getName();
      byte[] row = append.getRow();
      // TODO optimimize
      Set<byte[]> families = append.getFamilyMap().keySet();
      // Invalidate list of family keys
      invalidateKeys(tableName, row, families);
      return null;
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * CHECKED 2.
   * 
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param families
   *          the families
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void invalidateKeys(byte[] tableName, byte[] row, Set<byte[]> families)
      throws IOException {

    for (byte[] family : families) {
      delete(tableName, row, family, null);
    }
  }

  /**
   * CHECKED 2 Post check and delete.
   * 
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param qualifier
   *          the qualifier
   * @param delete
   *          the delete
   * @param result
   *          the result
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public boolean preCheckAndDelete(HTableDescriptor tableDesc, byte[] row,
      byte[] family, byte[] qualifier, Delete delete, boolean result)
      throws IOException {

    doDeleteOperation(tableDesc, delete);
    return result;
  }

  /**
   * CHECKED 2 TODO: we ignore timestamps and delete everything delete
   * 'family:column' deletes all versions delete 'family' - deletes entire
   * family delete 'row' - deletes entire row from cache
   * 
   * We ignore time range and timestamps when we do delete from cache.
   * 
   * @param tableDesc
   *          the table desc
   * @param delete
   *          the delete
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void doDeleteOperation(HTableDescriptor tableDesc, Delete delete)
      throws IOException {

    if (isDisabled())
      return;

    try {
      mutationsInProgress.incrementAndGet();
      byte[] tableName = tableDesc.getName();
      byte[] row = delete.getRow();

      Set<byte[]> families = delete.getFamilyMap().keySet();
      if (families.size() == 0) {
        // we delete entire ROW
        families = getFamiliesForTable(tableDesc);
      }
      // Invalidate list of family keys

      invalidateKeys(tableName, row, families);
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * CHECKED 2 Get set of family names.
   * 
   * @param desc
   *          the desc
   * @return the families for table
   */
  private Set<byte[]> getFamiliesForTable(HTableDescriptor desc) {
    Set<byte[]> families = new HashSet<byte[]>();
    HColumnDescriptor[] fams = desc.getColumnFamilies();
    for (HColumnDescriptor cdesc : fams) {
      families.add(cdesc.getName());
    }
    return families;
  }

  /**
   * CHECKED 2 Generic delete operation for Row, Family, Column. It does not
   * report parent
   * 
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param family
   *          the family
   * @param column
   *          the column
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void delete(byte[] tableName, byte[] row, byte[] family, byte[] column)
      throws IOException {
    ByteBuffer buf = getLocalByteBuffer(); // bufTLS.get();
    // if(isTrace()){
    // LOG.error("Delete : tableName="+
    // new String(tableName)+" Family="+ new String(family) + " Col="+
    // ((column != null)?new String(column): null));
    // }
    prepareKeyForGet(buf, tableName, row, 0, row.length, family, column);

    try {
      rowCache.remove(buf);
    } catch (NativeMemoryException e) {
      throw new IOException(e);
    }

  }

  /**
   * CHECKED 2
   * 
   * Post checkAndPut.
   * 
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param qualifier
   *          the qualifier
   * @param put
   *          the put
   * @param result
   *          the result
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public boolean preCheckAndPut(HTableDescriptor tableDesc, byte[] row,
      byte[] family, byte[] qualifier, Put put, boolean result)
      throws IOException {

    // Do put if result of check is true
    doPutOperation(tableDesc, put);
    return result;
  }

  // /**
  // * Dump put.
  // *
  // * @param put the put
  // */
  // private void dumpPut(Put put) {
  // Map<byte[], List<KeyValue>> map = put.getFamilyMap();
  // for(byte[] row: map.keySet()){
  // List<KeyValue> list = map.get(row);
  // for(KeyValue kv : list){
  // LOG.error(kv);
  // }
  // }
  //		
  // }
  /**
   * CHECKED 2 Do put operation.
   * 
   * @param tableDesc
   *          the table desc
   * @param put
   *          the put
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void doPutOperation(HTableDescriptor tableDesc, Put put)
      throws IOException {

    // LOG.error("PrePUT executed \n ");
    // /*DEBUG*/dumpPut(put);

    if (isDisabled())
      return;

    try {
      mutationsInProgress.incrementAndGet();
      byte[] tableName = tableDesc.getName();
      byte[] row = put.getRow();

      Set<byte[]> families = put.getFamilyMap().keySet();

      // Invalidate list of family keys

      invalidateKeys(tableName, row, families);
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * CHECKED 2 Post get operation: 1. We update data in cache
   * 
   * @param tableDesc
   *          the table desc
   * @param get
   *          the get
   * @param results
   *          the results
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public void postGet(HTableDescriptor tableDesc, Get get,
      List<KeyValue> results) throws IOException {

    try {
      // TODO with postGet and disabled

      if (isDisabled()) {
        return;
      }

      // Check if we bypass cache
      RequestContext ctxt = contextTLS.get();
      if (ctxt.isBypassCache()) {
        // bypass
        return;
      }
      if(isTrace()) {
        LOG.info("[postGet] "+ get);
      }

      // 1. Make sure we sorted kv's out
      // TODO do we sorting?
      // FIXME: confirm that we always get results sorted
      // Collections.sort(results, KeyValue.COMPARATOR);
      // 2. Next iterate results by columnFamily
      List<KeyValue> bundle = new ArrayList<KeyValue>();
      byte[] row = get.getRow();
      for (int index = 0; index < results.size();) {
        index = processFamilyForPostGet(index, results, row, tableDesc, bundle);
        bundle.clear();
      }

    } finally {
      filterResults(results, tableDesc);
      resetRequestContext();
    }
  }

  /**
   * FIXME - optimize CHECKED2 Filter results in postGet.
   * 
   * @param results
   *          the results
   * @param tableDesc
   *          the table desc
   */
  private void filterResults(List<KeyValue> results, HTableDescriptor tableDesc) {
    // results are sorted
    if (results.size() == 0)
      return;

    int index = 0;
    byte[] family = results.get(0).getFamily();
    byte[] column = results.get(0).getQualifier();
    // FIXME - optimize TTL
    int ttl = tableDesc.getFamily(family).getTimeToLive();

    int count = 0;

    while (index < results.size()) {

      KeyValue kv = results.get(index);
      byte[] fam = kv.getFamily();
      byte[] col = kv.getQualifier();
      if (Bytes.equals(family, fam) == false) {
        family = fam;
        ttl = tableDesc.getFamily(family).getTimeToLive();
        count = 0;
      }
      if (Bytes.equals(column, col) == false) {
        column = col;
        count = 0;
      }
      count++;
      if (doFilter(kv, count) == false) {
        // check TTL
        if (kv.getTimestamp() < System.currentTimeMillis() - ((long) ttl)
            * 1000) {
          // LOG.error("FILTERED: "+kv);
          results.remove(index);
          continue;
        }
      } else {
        // LOG.error("FILTERED: "+kv);
        results.remove(index);
        continue;
      }
      // LOG.error("PASSED: "+kv);
      index++;
    }

  }

  /**
   * CHECKED 2 Processing family in postGet. All K-V's are sorted and all K-V's
   * from the same family are arranged together.
   * 
   * @param index
   *          the index
   * @param results
   *          the results
   * @param row
   *          the row
   * @param tableDesc
   *          the table desc
   * @param bundle
   *          the bundle
   * @return the int
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private int processFamilyForPostGet(int index, List<KeyValue> results,
      byte[] row, HTableDescriptor tableDesc, List<KeyValue> bundle)
      throws IOException {

    byte[] family = getFamily(results.get(index));
    // *DEBUG*/ LOG.info("processFamilyForPostGet: "+ new String(family));
    return processFamilyColumnForAdd(index, results, tableDesc, row, family,
        bundle);
  }

  /**
   * CHECKED 2 This method actually inserts/updates KEY = 'table:rowkey:family'
   * with all versions in a given bundle. It updates family KEY =
   * 'table:rowkey:family' as well to keep track of all cached columns
   * 
   * @param index
   *          the index
   * @param results
   *          the results
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param bundle
   *          the bundle
   * @return the int
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private int processFamilyColumnForAdd(int index, List<KeyValue> results,
      HTableDescriptor tableDesc, byte[] row, byte[] family,
      List<KeyValue> bundle) throws IOException {

    byte[] tableName = tableDesc.getName();

    // 1. get column
    while (index < results.size()) {
      byte[] fam = getFamily(results.get(index));

      if (Bytes.equals(fam, family) != true) {
        // We finished family
        break;
      }

      byte[] col = getColumn(results.get(index));
      // scan results until we get other family, column
      for (; index < results.size(); index++) {
        KeyValue kv = results.get(index);
        int familyOffset = kv.getFamilyOffset();
        int familyLength = kv.getFamilyLength();
        int columnOffset = kv.getQualifierOffset();
        int columnLength = kv.getQualifierLength();

        if (Bytes.equals(col, 0, col.length, kv.getBuffer(), columnOffset,
            columnLength)
            && Bytes.equals(family, 0, family.length, kv.getBuffer(),
                familyOffset, familyLength)) {
          bundle.add(kv);
        } else {
          break;
        }

      }
    }
    // We do caching ONLY if ROWCACHE is set for 'table' or 'cf'
    if (isRowCacheEnabledForFamily(tableDesc, family)) {
      // Do only if it ROWCACHE is set for the family
      upsertFamilyColumns(tableName, row, family, bundle);
    }

    return index;
  }

  /**
   * CHECKED 2 TODO - optimize filters Do filter.
   * 
   * @param kv
   *          the kv
   * @param count
   *          the count
   * @return true, if successful
   */
  private boolean doFilter(KeyValue kv, int count) {
    // 1. Check timeRange
    RequestContext context = contextTLS.get();
    TimeRange timeRange = context.getTimeRange();

    int maxVersions = context.getMaxVersions();
    Filter filter = context.getFilter();

    // We skip families and columns before we call filter
    // if(doFamilyMapFilter(kv, context) == true){
    // return true;
    // }

    if (shouldSkipColumn(kv.getFamily(), kv.getQualifier())) {
      return true;
    }

    if (timeRange != null) {
      if (timeRange.compare(kv.getTimestamp()) != 0) {
        return true;
      }
    }
    // 2. Check maxVersions
    if (count > maxVersions)
      return true;

    // 3. Check filter
    if (filter != null) {
      ReturnCode code = filter.filterKeyValue(kv);
      if (code == ReturnCode.INCLUDE) {
        return false;
      } else if (code == ReturnCode.INCLUDE_AND_NEXT_COL) {
        // TODO we do not interrupt iteration. The filter
        // implementation MUST be tolerant
        return false;
      }
    } else {
      return false;
    }
    // Meaning : filter != null and filter op result is not success.
    return true;
  }

  // private KeyValue doFilter(ByteBuffer buf, int count, byte[] row, byte[]
  // family, byte[] column) {
  // // 1. Check timeRange
  // KeyValue kv = null;
  // try {
  // RequestContext context = contextTLS.get();
  // TimeRange timeRange = context.getTimeRange();
  //
  // int maxVersions = context.getMaxVersions();
  // Filter filter = context.getFilter();
  //
  // // We skip families and columns before we call filter
  // // if(doFamilyMapFilter(kv, context) == true){
  // // return true;
  // // }
  // long ts = buf.getLong();
  // if (timeRange != null) {
  // if (timeRange.compare(ts) != 0) {
  // return null;
  // }
  // }
  // // 2. Check maxVersions
  // if (count > maxVersions)
  // return null;
  //
  // // 3. Check filter
  //
  // kv = readKeyValue(buf, row, family, column, ts);
  // if (filter != null) {
  //
  // ReturnCode code = filter.filterKeyValue(kv);
  // if (code == ReturnCode.INCLUDE) {
  // return kv;
  // } else if (code == ReturnCode.INCLUDE_AND_NEXT_COL) {
  // // TODO we do not interrupt iteration. The filter
  // // implementation MUST be tolerant
  // return kv;
  // }
  // } else {
  // return kv;
  // }
  // // Meaning : filter != null and filter op result is not success.
  // return kv;
  // } finally {
  // if( kv == null){
  // // skip
  // int size = buf.getInt();
  // skip(buf, size);
  // }
  // }
  // }

  // private KeyValue readKeyValue(ByteBuffer buf, byte[] row, byte[] family,
  // byte[] column, long ts) {
  // int size = buf.getInt();
  // byte[] value = new byte[size];
  // buf.get(value);
  // return new KeyValue(row, family, column, ts, value);
  // }

  /**
   * Check if we need to filer K-V out based on original Get request.
   * 
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param family
   *          the family
   * @param bundle
   *          the bundle
   * @return true - if filter, false otherwise
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  // private boolean doFamilyMapFilter(KeyValue kv, RequestContext context) {
  // Map<byte[], NavigableSet<byte[]>> map = context.getFamilyMap();
  // if (map == null) return false;
  // byte[] family = kv.getFamily();
  // NavigableSet<byte[]> cols = map.get(family);
  // if( cols == null || cols.size() == 0) return false;
  // byte[] col = kv.getQualifier();
  // if(cols.contains(col)) return false;
  // return true;
  // }

  /**
   * CHECKED 2
   * 
   * Sets the family column. Format:
   * 
   * // List of columns: // 4 bytes - total columns // Column: // 4 bytes -
   * qualifier length // qualifier // 4 bytes - total versions // list of
   * versions: // { 8 bytes - timestamp // 4 bytes - value length // value // }
   * 
   * @param tableName
   *          the table name
   * @param family
   *          the family
   * @param bundle
   *          the bundle
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private void upsertFamilyColumns(byte[] tableName, byte[] row, byte[] family,
      List<KeyValue> bundle) throws IOException {
    if (bundle.size() == 0)
      return;
    // Get first
    ByteBuffer buf = getLocalByteBuffer();// bufTLS.get();

    try {
      prepareKeyForPut(buf, tableName, row, 0, row.length, family, null);

      // buffer position is at beginning of a value block
      int valueSize = Bytes.SIZEOF_INT;
      int numColumns = 0;
      int startPosition = buf.position();
      // Skip numColumns
      skip(buf, Bytes.SIZEOF_INT);
      while (bundle.size() > 0) {
        valueSize += addColumn(buf, bundle);
        numColumns++;
      }
      buf.putInt(startPosition, numColumns);
      // Update value len
      buf.putInt(4, valueSize);
      // Now we have K-V pair in a buffer - put it into cache
    } catch (BufferOverflowException e) {
      LOG
          .error("[row-cache] Ignore put op. The row:family is too large and exceeds the limit "
              + ioBufferSize + " bytes.");
      buf.clear();
      return;
    }
    doPut(buf);

  }

  // private void upsertFamilyColumns(byte[] tableName, byte[] row, byte[]
  // family,
  // List<KeyValue> bundle) throws IOException
  // {
  // if(bundle.size() == 0) return;
  // // Get first
  // ByteBuffer buf = bufTLS.get();
  // long bufptr = NativeMemory.getBufferAddress(buf);
  // long start = bufptr;
  // buf.clear();
  // int off = prepareKeyForPut(bufptr, tableName, row, 0, row.length, family,
  // null);
  // bufptr = start + off;
  // // buffer position is at beginning of a value block
  // int valueSize= Bytes.SIZEOF_INT;
  // int numColumns = 0;
  // long startPosition = bufptr;
  // // Skip numColumns
  // bufptr += Bytes.SIZEOF_INT;
  // while( bundle.size() > 0){
  //			
  // int vSize = addColumn(bufptr, bundle);
  // bufptr += vSize;
  // valueSize += vSize;
  // numColumns ++;
  //
  // }
  //
  // Unsafe unsafe = NativeMemory.getUnsafe();
  // unsafe.putInt(startPosition, numColumns);
  // // Update value len
  // unsafe.putInt(start + 4, valueSize);
  // // Now we have K-V pair in a buffer - put it into cache
  //		
  // doPut(buf);
  //				
  // }

  // /**
  // * Dump key value put.
  // *
  // * @param buf the buf
  // */
  // private void dumpKeyValuePut(ByteBuffer buf)
  // {
  // LOG.info("PUT:");
  // int pos = buf.position();
  //		
  // buf.position(0);
  // int keySize = buf.getInt();
  // LOG.info("key_size="+keySize);
  // int total = 0;
  // buf.position(8);
  // int len = buf.getShort();
  // byte[] tableName = new byte[len];
  // buf.get(tableName);
  // LOG.info("table_name="+new String(tableName));
  // total += 2 + len;
  // len = buf.getShort();
  // byte[] row = new byte[len];
  // buf.get(row);
  // LOG.info("row ="+ new String(row));
  // total += 2 + len;
  // if( total < keySize){
  // len = buf.getShort();
  // byte[] fam = new byte[len];
  // buf.get(fam);
  // total += 2+ len;
  // LOG.info("family ="+new String(fam));
  // if( total < keySize){
  // len = buf.getInt();
  // total+= 4 + len;
  // byte[] col = new byte[len];
  // buf.get(col);
  // LOG.info("column=" + new String(col));
  // }
  //			
  // }
  //		
  // buf.position(pos);
  // }

  /**
   * Dump key value get.
   * 
   * @param buf
   *          the buf
   * @param bundle
   *          the bundle
   * @return the int
   */
  // private void dumpKeyValueGet(ByteBuffer buf)
  // {
  // LOG.info("GET:");
  // int pos = buf.position();
  // buf.position(0);
  // int keySize = buf.getInt();
  // LOG.info("key_size="+keySize);
  // int total = 0;
  // //buf.position(8);
  // int len = buf.getShort();
  // byte[] tableName = new byte[len];
  // buf.get(tableName);
  // LOG.info("table_name="+new String(tableName));
  // total += 2 + len;
  // len = buf.getShort();
  // byte[] row = new byte[len];
  // buf.get(row);
  // LOG.info("row ="+ new String(row));
  // total += 2 + len;
  // if( total < keySize){
  // len = buf.getShort();
  // byte[] fam = new byte[len];
  // buf.get(fam);
  // total += 2+ len;
  // LOG.info("family ="+new String(fam));
  // if( total < keySize){
  // len = buf.getInt();
  // total+= 4 + len;
  // byte[] col = new byte[len];
  // buf.get(col);
  // LOG.info("column=" + new String(col));
  // }
  //			
  // }
  //		
  // buf.position(pos);
  // }

  /**
   * CHECKED 2 add column.
   * 
   * @param buf
   *          the buf
   * @param bundle
   *          the bundle
   * @return the int
   */
  private int addColumn(ByteBuffer buf, List<KeyValue> bundle) {
    if (bundle.size() == 0)
      return 0;

    byte[] column = getColumn(bundle.get(0));
    byte[] col = column;
    buf.putInt(col.length);
    buf.put(col);
    int startPosition = buf.position();
    int totalVersions = 0;
    // size = col (4) + num versions (4) + col length
    int size = 2 * Bytes.SIZEOF_INT + col.length;
    // Skip total versions
    skip(buf, Bytes.SIZEOF_INT);

    while (Bytes.equals(column, col)) {
      size += addKeyValue(buf, bundle.get(0));
      totalVersions++;
      bundle.remove(0);
      if (bundle.size() == 0)
        break;
      col = getColumn(bundle.get(0));
    }
    // Put total versions
    buf.putInt(startPosition, totalVersions);

    return size;
  }

  // private int addColumn(long buf, List<KeyValue> bundle) {
  // if( bundle.size() == 0) return 0;
  //		
  // Unsafe unsafe = NativeMemory.getUnsafe();
  // byte[] column = getColumn(bundle.get(0));
  // byte[] col = column;
  // //buf.putInt(col.length);
  // unsafe.putInt(buf, col.length);
  // buf += 4;
  // //buf.put(col);
  // NativeMemory.memcpy(column, 0, column.length, buf, 0);
  // buf += column.length;
  // long startPosition = buf;//buf.position();
  // int totalVersions = 0;
  // // size = col (4) + num versions (4) + col length
  // int size = 2 * Bytes.SIZEOF_INT + col.length;
  // // Skip total versions
  // //skip(buf, Bytes.SIZEOF_INT);
  // buf += Bytes.SIZEOF_INT;
  // while(Bytes.equals(column, col)){
  // int lsize = addKeyValue(buf, bundle.get(0), unsafe);
  // size += lsize; buf += lsize;
  // totalVersions++;
  // bundle.remove(0);
  // if(bundle.size() == 0) break;
  // col = getColumn(bundle.get(0));
  // }
  // // Put total versions
  // //buf.putInt(startPosition, totalVersions);
  // unsafe.putInt(startPosition, totalVersions);
  // return size;
  // }

  /**
   * CHECKED 2 Do put.
   * 
   * @param kv
   *          the kv
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private final void doPut(ByteBuffer kv) throws IOException {
    try {
      rowCache.putCompress(kv, OffHeapCache.NO_EXPIRE);
    } catch (NativeMemoryException e) {
      LOG.error("[row-cache] Put failed", e);
    }
  }

  /**
   * CHECKED 2 Adds the key value (KeyValue) to a buffer for Put/Append.
   * 
   * @param buf
   *          the buf
   * @param kv
   *          the kv
   * @return the int
   */
  private int addKeyValue(ByteBuffer buf, KeyValue kv) {

    // Format:
    // 8 bytes - ts
    // 4 bytes - value length
    // value blob
    int valLen = kv.getValueLength();
    int size = 12 + valLen;
    buf.putLong(kv.getTimestamp());
    buf.putInt(valLen);
    buf.put(kv.getBuffer(), kv.getValueOffset(), valLen);
    return size;
  }

  // private int addKeyValue(long buf, KeyValue kv, Unsafe unsafe) {
  //
  // // Format:
  // // 8 bytes - ts
  // // 4 bytes - value length
  // // value blob
  // int valLen = kv.getValueLength();
  // int size = 12 + valLen;
  // //buf.putLong(kv.getTimestamp());
  // unsafe.putLong(buf, kv.getTimestamp());
  // buf += 8;
  // //buf.putInt(valLen);
  // unsafe.putInt(buf, valLen);
  // buf += 4;
  // //buf.put(kv.getBuffer(), kv.getValueOffset(), valLen);
  // NativeMemory.memcpy(kv.getBuffer(), kv.getValueOffset(), valLen, buf, 0);
  // return size;
  // }
  /**
   * CHECKED 2 This involves an array copy.
   * 
   * @param kv
   *          the kv
   * @return - column name
   */
  private byte[] getColumn(KeyValue kv) {
    int off = kv.getQualifierOffset();
    int len = kv.getQualifierLength();
    byte[] buf = new byte[len];
    System.arraycopy(kv.getBuffer(), off, buf, 0, len);
    return buf;
  }

  /**
   * CHECKED 2 This involves an array copy.
   * 
   * @param kv
   *          the kv
   * @return the family
   */
  private final byte[] getFamily(KeyValue kv) {
    int off = kv.getFamilyOffset();
    int len = kv.getFamilyLength();
    byte[] buf = new byte[len];
    System.arraycopy(kv.getBuffer(), off, buf, 0, len);
    return buf;
  }

  /**
   * CHECKED 2 Post increment.
   * 
   * @param tableDesc
   *          the table desc
   * @param increment
   *          the increment
   * @param result
   *          the result
   * @return the result
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public Result preIncrement(HTableDescriptor tableDesc, Increment increment,
      Result result) throws IOException {

    if (isTrace()) {
      LOG.info("[row-cache] preIncrement: " + increment + " disabled="
          + isDisabled());
    }
    if (isDisabled())
      return null;

    try {
      mutationsInProgress.incrementAndGet();
      // if(result == null || result.isEmpty()) return result;
      byte[] tableName = tableDesc.getName();
      byte[] row = increment.getRow();

      Set<byte[]> families = increment.familySet();
      // Invalidate list of family keys
      invalidateKeys(tableName, row, families);
      return result;
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * CHECKED 2 Post increment column value.
   * 
   * @param tableDesc
   *          the table desc
   * @param row
   *          the row
   * @param family
   *          the family
   * @param qualifier
   *          the qualifier
   * @return the long
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public long preIncrementColumnValue(HTableDescriptor tableDesc, byte[] row,
      byte[] family, byte[] qualifier) throws IOException {

    if (isDisabled())
      return 0;
    try {
      mutationsInProgress.incrementAndGet();

      byte[] tableName = tableDesc.getName();
      // Invalidate family - column
      delete(tableName, row, family, null);
      return 0;
    } finally {
      mutationsInProgress.decrementAndGet();
    }
  }

  /**
   * CHECKED 2 Post delete.
   * 
   * @param tableDesc
   *          the table desc
   * @param delete
   *          the delete
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void preDelete(HTableDescriptor tableDesc, Delete delete)
      throws IOException {
    // if(RowCache.isDisabled()) return ;
    doDeleteOperation(tableDesc, delete);
  }

  /**
   * CHECKED 2 TODO : optimize Pre exists call.
   * 
   * @param tableDesc
   *          the table desc
   * @param get
   *          the get
   * @param exists
   *          the exists
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */

  public boolean preExists(HTableDescriptor tableDesc, Get get, boolean exists)
      throws IOException {

    if (isDisabled())
      return exists;

    List<KeyValue> results = new ArrayList<KeyValue>();
    try {
      // LOG.error("Exists? "+get);
      preGet(tableDesc, get, results);
      boolean result = results.size() > 0 ? true : false;
      ;
      // LOG.error("Exists="+result);
      return result;
    } finally {
      resetRequestContext();
    }
  }

  // /**
  // * CHECKED 2
  // * Reads column from cached row:family.
  // *
  // * @param tableName the table name
  // * @param row the row
  // * @param columnFamily the column family
  // * @param column the column
  // * @return the list
  // */
  // private List<KeyValue> readColumn(byte[] tableName, byte[] row, byte[]
  // columnFamily, byte[] column)
  // {
  // List<KeyValue> result = new ArrayList<KeyValue>();
  // ByteBuffer buf = bufTLS.get();
  // prepareKeyForGet(buf, tableName, row, 0, row.length, columnFamily, null);
  // LOG.info("readColumn call:");
  // /*DEBUG*/dumpKeyValueGet(buf);
  // if(isNotNull(buf) == false){
  // // Return empty list
  // return result;
  // }
  //		
  // // Now we have in buffer:
  // // 4 bytes - total length
  // // 8 bytes - address
  // // List of columns:
  // // 4 bytes - total columns
  // // Column:
  // // 4 bytes - qualifier length
  // // qualifier
  // // 4 bytes - total versions
  // // list of versions:
  // // { 8 bytes - timestamp
  // // 4 bytes - value length
  // // value
  // // }
  // int offset = 12;
  // buf.position(offset);
  // int off = findColumn(buf, column);
  // if( off >= 0){
  // buf.position(12 + off);
  // result = readColumn(buf, row, columnFamily, result);
  // }
  //		
  // return result;
  // }

  // /**
  // * Finds column in row:family.
  // *
  // * @param buf the buf
  // * @param column the column
  // * @return the int
  // */
  // private int findColumn(ByteBuffer buf, byte[] column) {
  //		
  // int startPosition = buf.position();
  // int totalColumns = buf.getInt();
  //		
  // int i = 0;
  // while( i++ < totalColumns){
  // int colSize = buf.getInt();
  // if( colSize != column.length){
  // buf.position(buf.position() - Bytes.SIZEOF_INT);
  // skipColumn(buf);
  // continue;
  // }
  // byte[] col = new byte[colSize];
  // buf.get(col);
  // if(Bytes.equals(column , col) ){
  // // found column
  // // rewind back by colSize + 4
  // skip(buf, - (colSize + Bytes.SIZEOF_INT));
  // return buf.position() - startPosition;
  // }
  //
  // }
  // return -1;
  // }

  // /**
  // * CHECKED 2
  // * Skips column.
  // *
  // * @param buf the buf
  // */
  // private void skipColumn(ByteBuffer buf) {
  // int colSize = buf.getInt();
  // // Skip
  // skip(buf, colSize);
  // //buf.position(buf.position() + colSize);
  // int totalVersions = buf.getInt();
  // int i =0;
  // while( i++ < totalVersions){
  // // Skip versions
  // // Skip ts
  // skip(buf, Bytes.SIZEOF_LONG);
  // int valueSize = buf.getInt();
  // skip(buf, valueSize);
  // }
  // }

  /**
   * CHECKED 2.
   * 
   * @param buf
   *          the buf
   * @param skip
   *          the skip
   */
  private final void skip(ByteBuffer buf, int skip) {
    buf.position(buf.position() + skip);
  }

  

  /**
   * Reads content of table:row:family.
   * 
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param columnFamily
   *          the column family
   * @return the list
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private List<KeyValue> readFamily(byte[] tableName, byte[] row,
      byte[] columnFamily) throws IOException {
    List<KeyValue> result = new ArrayList<KeyValue>();
    ByteBuffer buf = getLocalByteBuffer();// bufTLS.get();
    long bufptr = NativeMemory.getBufferAddress(buf);
    prepareKeyForGet(bufptr, tableName, row, 0, row.length, columnFamily, null);

    try {
      rowCache.getDecompress(buf);
    } catch (NativeMemoryException e) {
      throw new IOException(e);
    }

    if (isNotNull(buf) == false) {
      // Return empty list
      // *DEBUG*/LOG.info("not found for: "+new String(row)+":"+new
      // String(columnFamily));
      return result;
    }

    // Now we have in buffer:
    // 4 bytes - total length
    // 8 bytes - address
    // List of columns:
    // 4 bytes - total columns
    // Column:
    // 4 bytes - qualifier length
    // qualifier
    // 4 bytes - total versions
    // list of versions:
    // { 8 bytes - timestamp
    // 4 bytes - value length
    // value
    // }
    int offset = 12;
    buf.position(offset);
    int totalColumns = buf.getInt();
    int i = 0;
//    if(isTrace()){
///*DEBUG*/LOG.info(" A:"+ buf.getLong(4)+" TL:"+ buf.getInt(0)+" TC:"+totalColumns);
//    }
    bufptr += 16;
    while (i++ < totalColumns) {
      // result = readColumn(buf, row, columnFamily, result);
      bufptr = readColumn(bufptr, row, columnFamily, result);
    }

    return result;
  }

  // /**
  // * We skip column which is not a part of a request
  // * @param buf
  // */
  // private void skipColumn(ByteBuffer buf){
  // int csize = buf.getInt();
  // skip(buf, csize);
  // int totalVersions = buf.getInt();
  // int i = 0;
  //
  // while( i++ < totalVersions){
  // // Read ts
  // buf.getLong();
  // int valueSize = buf.getInt();
  // skip(buf, valueSize);
  // }
  //
  // }

  /**
   * We skip column which is not a part of a request.
   * 
   * @param buf
   *          the buf
   * @return the long
   */
  private long skipColumn(long buf) {
    Unsafe unsafe = NativeMemory.getUnsafe();

    int csize = unsafe.getInt(buf);
    buf += 4 + csize;

    int totalVersions = unsafe.getInt(buf);
    buf += 4;
    int i = 0;

    while (i++ < totalVersions) {
      buf += 8;
      int valueSize = unsafe.getInt(buf);
      buf += 4 + valueSize;
    }
    return buf;
  }

  /**
   * CHECKED 2.
   * 
   * @param bufptr
   *          the bufptr
   * @param row
   *          the row
   * @param family
   *          the family
   * @param result
   *          the result
   * @return the list
   */
  // private List<KeyValue> readColumn(ByteBuffer buf, byte[] row, byte[]
  // family, List<KeyValue> result) {
  //		
  // // Column format
  // // Column:
  // // 4 bytes - qualifier length
  // // qualifier
  // // 4 bytes - total versions
  // // list of versions:
  // // { 8 bytes - timestamp
  // // 4 bytes - value length
  // // value
  // // }
  // long bufptr = NativeMemory.getBufferAddress(buf);
  //		
  // long ptr = bufptr + buf.position();
  //		
  // int csize = buf.getInt();
  // byte[] column = new byte[csize];
  // buf.get(column);
  //		
  // //bufptr += 4 + csize;
  //		
  // if(shouldSkipColumn(family, column)){
  // skip(buf, -(csize + 4));
  // ptr = skipColumn(ptr);
  // buf.position((int)(ptr - bufptr));
  // return result;
  // }
  //		
  // int totalVersions = buf.getInt();
  // int i = 0;
  //
  // while( i++ < totalVersions){
  // KeyValue kv = null;
  // if( (kv = doFilter(buf, i, row, family, column)) != null) {
  // result.add(kv);
  // }
  // }
  //
  // return result;
  // }

  /**
   * CHECKED 2.
   * 
   * @param buf
   *          the buf
   * @param row
   *          the row
   * @param family
   *          the family
   * @param result
   *          the result
   * @return the list
   */
  private long readColumn(long bufptr, byte[] row, byte[] family,
      List<KeyValue> result) {

    // Column format
    // Column:
    // 4 bytes - qualifier length
    // qualifier
    // 4 bytes - total versions
    // list of versions:
    // { 8 bytes - timestamp
    // 4 bytes - value length
    // value
    // }
    Unsafe unsafe = NativeMemory.getUnsafe();

    int csize = unsafe.getInt(bufptr);
//    if(isTrace()) LOG.info("ROW:"+ new String(row) + " FAM:"+new String(family) +" col ptr:"+bufptr+" col size:"+csize
//        );
    byte[] column = new byte[csize];
    NativeMemory.memcpy(bufptr, 4, column, 0, csize);
    bufptr += 4 + csize;
    // boolean skip =
    if (shouldSkipColumn(family, column)) {
      bufptr -= 4 + csize;
      bufptr = skipColumn(bufptr);
      return bufptr;
    }

    int totalVersions = unsafe.getInt(bufptr);
    bufptr += 4;
    int i = 0;
    RequestContext context = contextTLS.get();
    TimeRange timeRange = context.getTimeRange();

    int maxVersions = context.getMaxVersions();
    Filter filter = context.getFilter();

    while (i++ < totalVersions) {
      KeyValue kv = null;
      // Read ts
      long ts = unsafe.getLong(bufptr);
      bufptr += 8;
      if (timeRange != null) {
        if (timeRange.compare(ts) != 0) {
          // LOG.error("Filtered range : i="+i+" ts="+ts+" range="+timeRange);
          bufptr = skipKeyValue(bufptr, unsafe);
          continue;
        }
      }
      // 2. Check maxVersions
      if (i > maxVersions) {
        bufptr = skipKeyValue(bufptr, unsafe);
        // LOG.error("Filtered maxVersions : i="+i+" maxVer="+maxVersions);
        continue;
      }

      // 3. Read value

      int size = unsafe.getInt(bufptr);
//      if(isTrace()){
//      /*DEBUG*/ LOG.info("value size:"+size);
//      }

      byte[] value = new byte[size];
      NativeMemory.memcpy(bufptr, 4, value, 0, size);
      bufptr += 4 + size;

      kv = new KeyValue(row, family, column, ts, value);

      if (filter != null) {
        ReturnCode code = filter.filterKeyValue(kv);
        if (code == ReturnCode.INCLUDE) {
          result.add(kv);
        } else if (code == ReturnCode.INCLUDE_AND_NEXT_COL) {
          // TODO we do not interrupt iteration. The filter
          // implementation MUST be tolerant
          result.add(kv);
        }
      } else {
        // LOG.error("Found:"+kv);
        result.add(kv);
      }

    }

    return bufptr;
  }

  /**
   * Skip key value.
   * 
   * @param bufptr
   *          the bufptr
   * @param unsafe
   *          the unsafe
   * @return the long
   */
  private final long skipKeyValue(long bufptr, Unsafe unsafe) {
    return bufptr + 4 + unsafe.getInt(bufptr);
  }

  /**
   * FIXME - optimize.
   * 
   * @param family
   *          the family
   * @param column
   *          the column
   * @return true, if successful
   */
  private boolean shouldSkipColumn(byte[] family, byte[] column) {

    RequestContext context = contextTLS.get();
    Map<byte[], NavigableSet<byte[]>> map = context.getFamilyMap();
    NavigableSet<byte[]> cols = map.get(family);
    if (cols == null || cols.size() == 0)
      return false;
    return cols.contains(column) == false;

  }

  /**
   * CHECKED 2 Pre get.
   * 
   * @param desc
   *          the desc
   * @param get
   *          the get
   * @param results
   *          the results
   * @return true if bypass HBase, false otherwise
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public boolean preGet(HTableDescriptor desc, Get get, List<KeyValue> results)
      throws IOException {

    if (isDisabled())
      return false;
    if (isTrace()){
      LOG.info("[row-cache][trace][preGet]: " + get);
    }
    updateRequestContext(get);

    RequestContext ctxt = contextTLS.get();
    if (isTrace()) {
      Map<byte[], NavigableSet<byte[]>> map = ctxt.getFamilyMap();
      for (byte[] f : map.keySet()) {
        NavigableSet<byte[]> set = map.get(f);
        LOG.info("[row-cache] " + new String(f) + " has " + (set != null? map.get(f).size(): null)
            + " columns");
      }
    }
    if (ctxt.isBypassCache()) {
      // bypass
      return false;
    }

    byte[] tableName = desc.getName();

    Set<byte[]> set = get.familySet();

    if (get.hasFamilies() == false) {
      set = getFamiliesForTable(desc);
      addFamilies(get, set);
    }

    List<byte[]> toDelete = new ArrayList<byte[]>();

    for (byte[] columnFamily : set) {
      if (isRowCacheEnabledForFamily(desc, columnFamily)) {
        if (processFamilyPreGet(desc, get, tableName, columnFamily, results)) {
          toDelete.add(columnFamily);
        }
      }

    }

    // Delete families later to avoid concurrent modification exception
    deleteFrom(get, toDelete);

    if (isTrace()){
      LOG.info("[row-cache][trace][preGet] found " + results.size());
    }
    // DEBUG ON
    fromCache = results.size();
    // DEBUG OFF
    // Now check if we need bypass() HBase?
    // 1. When map.size == 0 (Get is empty - then we bypass HBase)
    if (get.getFamilyMap().size() == 0) {
      // Reset request context
      if(isTrace()){
        LOG.error("PreGet bypass HBase");
      }
      resetRequestContext();
      return true;
    }
    if (isTrace())
      LOG.info("[row-cache][trace][preGet]: send to HBase " + get);
    // Finish preGet
    return false;
  }

  /**
   * Delete from.
   * 
   * @param get
   *          the get
   * @param toDelete
   *          the to delete
   */
  private void deleteFrom(Get get, List<byte[]> toDelete) {
    Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
    for (byte[] f : toDelete) {
      map.remove(f);
    }
  }

  /**
   * CHECKED 2. Process family pre get.
   * 
   * @param desc
   *          the desc
   * @param get
   *          the get
   * @param tableName
   *          the table name
   * @param columnFamily
   *          the column family
   * @param results
   *          the results
   * @return true, if successful
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  private boolean processFamilyPreGet(HTableDescriptor desc, Get get,
      byte[] tableName, byte[] columnFamily, List<KeyValue> results)
      throws IOException {

    Map<byte[], NavigableSet<byte[]>> map = get.getFamilyMap();
    byte[] row = get.getRow();

    if (desc.hasFamily(columnFamily) == false) {
      // TODO, do we have to remove columnFamily?
      // If family does not exists - remove family from Get request
      map.remove(columnFamily);
      return true;
    }

    // FIXME - This is not safe
    byte[] key = Bytes.add(tableName, columnFamily);
    Integer ttl = null;
    synchronized (familyTTLMap) {
      ttl = familyTTLMap.get(key);
      if (ttl == null || ttl == 0) {
        HColumnDescriptor cdesc = desc.getFamily(columnFamily);
        ttl = cdesc.getTimeToLive();
        familyTTLMap.put(key, ttl);
      }
    }

    boolean foundFamily = false;
    List<KeyValue> res = readFamily(tableName, row, columnFamily);
    foundFamily = res.size() > 0;
    res = filterTTL(res, ttl);
    results.addAll(res);
    return foundFamily;
  }

  /**
   * CHECKED 2.
   * 
   * @param list
   *          the list
   * @param ttl
   *          the ttl
   * @return the list
   */
  private List<KeyValue> filterTTL(List<KeyValue> list, int ttl) {

    long oldestTimestamp = System.currentTimeMillis() - ((long) ttl) * 1000;
    for (int i = 0; i < list.size(); i++) {
      KeyValue kv = list.get(i);
      if (kv.getTimestamp() < oldestTimestamp) {
        list.remove(i);
        i--;
      }
    }
    return list;
  }

  /**
   * CHECKED 2.
   * 
   * @param get
   *          the get
   * @param families
   *          the families
   */

  private void addFamilies(Get get, Set<byte[]> families) {
    for (byte[] fam : families) {
      get.addFamily(fam);
    }
  }

  /**
   * CHECKED Resets all filter conditions in a current Get operation to a
   * default and max values This will get us ALL cell versions from HBase to
   * keep in memory Make sure that you are aware of consequences.
   * 
   * @param get
   *          - Get operation
   */
  private void updateRequestContext(Get get) {

    RequestContext context = contextTLS.get();
    boolean bypassCache = Hints.bypassCache(get, true);
    context.setBypassCache(bypassCache);

    context.setFamilyMap(get.getFamilyMap());

    context.setFilter(get.getFilter());
    get.setFilter(null);
    context.setTimeRange(get.getTimeRange());
    try {
      get.setTimeRange(0L, Long.MAX_VALUE);
      context.setMaxVersions(get.getMaxVersions());
      get.setMaxVersions(Integer.MAX_VALUE);
    } catch (IOException e) {
    }

  }

  /**
   * CHECKED 2 After preGet if successful or postGet. We need to make it public
   * API for testing only
   */
  public void resetRequestContext() {

    RequestContext context = contextTLS.get();

    context.setFilter(null);
    context.setMaxVersions(Integer.MAX_VALUE);
    context.setTimeRange(null);
    context.setBypassCache(false);
    context.setFamilyMap(null);

  }

  /**
   * CHECKED 2 Checks if is not null.
   * 
   * @param buf
   *          the buf
   * @return true, if is not null
   */
  private final boolean isNotNull(ByteBuffer buf) {
    return buf.getInt(0) != 0;
  }

  /**
   * CHECKED 2 Prepare key for Get op.
   * 
   * @param buf
   *          the buf
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param offset
   *          the offset
   * @param size
   *          the size
   * @param columnFamily
   *          the column family
   * @param column
   *          the column
   */
  private void prepareKeyForGet(ByteBuffer buf, byte[] tableName, byte[] row,
      int offset, int size, byte[] columnFamily, byte[] column) {

    buf.clear();
    int totalSize = 2 + tableName.length + // table
        2 + size + // row
        ((columnFamily != null) ? (2 + columnFamily.length) : 0) + // family
        ((column != null) ? (4 + column.length) : 0); // column
    buf.putInt(totalSize);
    // 4 bytes to keep key length;
    buf.putShort((short) tableName.length);
    buf.put(tableName);
    buf.putShort((short) size);
    buf.put(row, offset, size);
    if (columnFamily != null) {
      buf.putShort((short) columnFamily.length);
      buf.put(columnFamily);
    }
    if (column != null) {
      buf.putInt(column.length);
      buf.put(column);
    }
    // prepare for read
    // buf.flip();

  }

  /**
   * Prepare key for get.
   * 
   * @param buf
   *          the buf
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param offset
   *          the offset
   * @param size
   *          the size
   * @param columnFamily
   *          the column family
   * @param column
   *          the column
   */
  private void prepareKeyForGet(long buf, byte[] tableName, byte[] row,
      int offset, int size, byte[] columnFamily, byte[] column) {

    Unsafe unsafe = NativeMemory.getUnsafe();
    int totalSize = 2 + tableName.length + // table
        2 + size + // row
        ((columnFamily != null) ? (2 + columnFamily.length) : 0) + // family
        ((column != null) ? (4 + column.length) : 0); // column
    // buf.putInt(totalSize);
    unsafe.putInt(buf, totalSize);
    buf += 4;
    // 4 bytes to keep key length;
    // buf.putShort((short)tableName.length);
    unsafe.putShort(buf, (short) tableName.length);
    buf += 2;
    // buf.put(tableName);
    NativeMemory.memcpy(tableName, 0, tableName.length, buf, 0);
    buf += tableName.length;
    // buf.putShort((short)size);
    unsafe.putShort(buf, (short) size);
    buf += 2;
    NativeMemory.memcpy(row, offset, size, buf, 0);
    buf += size;

    if (columnFamily != null) {
      // buf.putShort((short) columnFamily.length);
      // buf.put(columnFamily);
      unsafe.putShort(buf, (short) columnFamily.length);
      buf += 2;
      NativeMemory.memcpy(columnFamily, 0, columnFamily.length, buf, 0);
      buf += columnFamily.length;

    }
    if (column != null) {
      // buf.putInt(column.length);
      // buf.put(column);
      unsafe.putShort(buf, (short) column.length);
      buf += 2;
      NativeMemory.memcpy(column, 0, column.length, buf, 0);
      buf += column.length;

    }
    // prepare for read
    // buf.flip();

  }

  /**
   * CHECKED 2 Prepare key for Get op.
   * 
   * @param buf
   *          the buf
   * @param tableName
   *          the table name
   * @param row
   *          the row
   * @param offset
   *          the offset
   * @param size
   *          the size
   * @param columnFamily
   *          the column family
   * @param column
   *          the column
   */
  private void prepareKeyForPut(ByteBuffer buf, byte[] tableName, byte[] row,
      int offset, int size, byte[] columnFamily, byte[] column) {

    buf.clear();
    int totalSize = 2 + tableName.length + // table
        2 + size + // row
        ((columnFamily != null) ? (2 + columnFamily.length) : 0) + // family
        ((column != null) ? (4 + column.length) : 0); // column
    buf.putInt(totalSize);
    // 4 bytes to keep key length;
    // skip 4 bytyes for Value length
    buf.position(8);
    buf.putShort((short) tableName.length);
    buf.put(tableName);
    buf.putShort((short) size);
    buf.put(row, offset, size);
    if (columnFamily != null) {
      buf.putShort((short) columnFamily.length);
      buf.put(columnFamily);
    }
    if (column != null) {
      buf.putInt(column.length);
      buf.put(column);
    }
    // prepare for read
    // buf.flip();

  }

  // private int prepareKeyForPut(long buf, byte[] tableName, byte[] row,
  // int offset, int size, byte[] columnFamily, byte[] column)
  // {
  //		
  //		
  // long start = buf;
  // Unsafe unsafe = NativeMemory.getUnsafe();
  // int totalSize = 2 + tableName.length + // table
  // 2 + size + // row
  // ((columnFamily != null)?(2 + columnFamily.length):0) + // family
  // ((column != null)? (4 + column.length):0); // column
  // //buf.putInt(totalSize);
  // unsafe.putInt(buf, totalSize);
  // buf += 8;
  // // 4 bytes to keep key length;
  // //buf.putShort((short)tableName.length);
  // unsafe.putShort(buf, (short)tableName.length);
  // buf += 2;
  // //buf.put(tableName);
  // NativeMemory.memcpy(tableName, 0, tableName.length, buf, 0);
  // buf += tableName.length;
  // //buf.putShort((short)size);
  // unsafe.putShort(buf, (short) size);
  // buf += 2;
  // NativeMemory.memcpy(row, offset, size, buf, 0);
  // buf += size;
  //		
  // if(columnFamily != null){
  // //buf.putShort((short) columnFamily.length);
  // //buf.put(columnFamily);
  // unsafe.putShort(buf, (short) columnFamily.length);
  // buf += 2;
  // NativeMemory.memcpy(columnFamily, 0, columnFamily.length, buf, 0);
  // buf += columnFamily.length;
  //			
  // }
  // if( column != null){
  // //buf.putInt(column.length);
  // //buf.put(column);
  // unsafe.putShort(buf, (short) column.length);
  // buf += 2;
  // NativeMemory.memcpy(column, 0, column.length, buf, 0);
  // buf += column.length;
  //			
  // }
  // return (int)(buf - start);
  //
  // }

  /**
   * CHECKED 2 Post put - do put operation.
   * 
   * @param tableDesc
   *          the table desc
   * @param put
   *          the put
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   */
  public void prePut(HTableDescriptor tableDesc, Put put) throws IOException {
    doPutOperation(tableDesc, put);
  }

  /**
   * Checks if is row cache enabled for family.
   * 
   * @param tableDesc
   *          the table desc
   * @param family
   *          the family
   * @return true, if is row cache enabled for family
   */
  private final boolean isRowCacheEnabledForFamily(HTableDescriptor tableDesc,
      byte[] family) {
    HColumnDescriptor colDesc = tableDesc.getFamily(family);
    // Its possible if request CF which does not exists
    if (colDesc == null)
      return false;

    byte[] value = colDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    // else check tableDesc
    value = tableDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    return false;
  }

  /**
   * Checks if is row cache enabled for family.
   * 
   * @param tableDesc
   *          the table desc
   * @param colDesc
   *          the col desc
   * @return true, if is row cache enabled for family
   */
  @SuppressWarnings("unused")
  private final boolean isRowCacheEnabledForFamily(HTableDescriptor tableDesc,
      HColumnDescriptor colDesc) {

    byte[] value = colDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    // else check tableDesc
    value = tableDesc.getValue(RConstants.ROWCACHE);
    if (value != null) {
      return Bytes.equals(value, RConstants.TRUE);
    }
    return false;
  }

  /**
   * Checks if is row cache enabled for families.
   * 
   * @param tableDesc
   *          the table desc
   * @param families
   *          the families
   * @return true, if is row cache enabled for families
   */
  private final boolean isRowCacheEnabledForFamilies(
      HTableDescriptor tableDesc, List<byte[]> families) {
    for (byte[] family : families) {
      if (isRowCacheEnabledForFamily(tableDesc, family)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Access to underlying off-heap cache.
   * 
   * @return the off heap cache
   */

  public OffHeapCache getOffHeapCache() {
    return rowCache;
  }

  /**
   * Size.
   * 
   * @return the long
   */
  public long size() {
    return rowCache.size();
  }

  /** DEBUG interface. */

  int fromCache;

  /**
   * Gets the from cache.
   * 
   * @return the from cache
   */
  public int getFromCache() {
    return fromCache;
  }
}
