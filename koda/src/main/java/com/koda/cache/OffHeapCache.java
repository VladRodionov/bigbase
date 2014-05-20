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
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;
import org.yamm.core.Malloc;

import sun.misc.Unsafe;

import com.koda.IOUtils;
import com.koda.KodaException;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.cache.AbstractScannerImpl.Prefetch;
import com.koda.cache.AbstractScannerImpl.PrefetchLocality;
import com.koda.cache.eviction.EvictionAlgo;
import com.koda.cache.eviction.EvictionData;
import com.koda.cache.eviction.EvictionFIFO;
import com.koda.cache.eviction.EvictionLFU;
import com.koda.cache.eviction.EvictionLRU;
import com.koda.cache.eviction.EvictionLRU2Q;
import com.koda.cache.eviction.EvictionListener;
import com.koda.cache.eviction.EvictionNone;
import com.koda.cache.eviction.EvictionPolicy;
import com.koda.cache.eviction.EvictionRandom;
import com.koda.cache.eviction.EvictionListener.Reason;
import com.koda.common.util.NumericHistogram;
import com.koda.compression.Codec;
import com.koda.compression.CodecFactory;
import com.koda.compression.CodecType;
import com.koda.compression.DeflateCodec;
import com.koda.compression.LZ4HCCodec;
import com.koda.config.CacheConfiguration;
import com.koda.config.Defaults;
import com.koda.config.ExtCacheConfiguration;
import com.koda.io.serde.SerDe;
import com.koda.persistence.DiskStore;
import com.koda.persistence.PersistenceMode;
import com.koda.persistence.ProgressListener;
import com.koda.persistence.rawfs.RawFSStore;
import com.koda.util.SpinLock;
import com.koda.util.SpinReadWriteLock;
import com.koda.util.Utils;
import com.koda.util.Value;

// TODO: Auto-generated Javadoc
//     TODO: Auto-generated Javadoc
// (+) TODO: NativeMemory.memread2a replacement after testing is done (Zero GC path) (+)
// (+?)TODO: cache pinning (pin item support - no eviction). Set expiration timeout = -1
// TODO: Finish persistent layer
// TODO: in process, out of process,  remote
// TODO: extreme low latency mode (OS latency elimination)
// TODO: Disruptor for out of process and remote
// TODO: RPC (in process code execution) - far beyond get/put
// TODO: size based eviction (?) - optimize eviction algo
// (+) TODO: replace Java hash with C hash (+)
// (+) TODO: lock free mode (for read only caches or for single thread execution) (+)
// TODO: optimistic locking (with version control). No locks on read/update, lock on remove/eviction
//       this can be done by application itself. (we need atomic operations support on native memory)
// TODO: in process cache striping (Disruptor N producers - 1 consumer (cache thread))
// TODO: out of process cache striping
// Cache striping with replication: For example - 8 stripes , 2 replicas. To suppress OS latency jitter
// to minimize OS related latency. We execute GET (or procedure call) in parallel
// TODO: Bootstrap interface (load data on initial startup) - cache loader. Persistence allows us to save/load
//       existing data, cache loader make initial cache load.
// (+)TODO: Store object into memory location, read object from memory location, store object to memory.
// TODO: POC: IP2GEO lookup, distributed DC (compression/decompression), optimistic locking through execute API and
//       versioned objects 
// TODO: Optimize PUT (native)
// (+)TODO: static ThreadLocal (done)
// TODO: Low Latency Toolkit (combination of Disruptor, OffHeapCache, OffHeap storage API and
//       other off heap data structures: Queue, Dequeue, priority queue, dynamic array, set, hash-set, tree-set )
//TODO: scan-resistent eviction algorithm


/**

 *  
 *  TODO (More compact represenation - minimize default overhead - 24+ bytes)
 *  
 *  1. Minimize memory overhead. (Use new ser API with blb size to get rid off obj length field) etc etc
 *  2. Extra large cache support (> 256M elements)
 *  3( +, no test yet). K, V serializers per cache. We cut 4+4 classids overhead
 *  4. OFFSET = 16 by default. Make it configurable (for example, 12 -when expiration is disabled, 
 *      8 -exp + no eviction)
 *  5. When KEY and VALUE of a the same size we can cut on KeyLength + ValueLength (8 bytes more)
 *  
 *  
 *  
 *  TODO
 
 *   10.(+) EvictionCandidateList is empty ? We need to make sure we do not fail 
 *   (in both: manual and auto modes)
 *   
 *   
 *   
 *   TODO (Eviction algorithms):
 *   
 *   1. Eviction breaks when there are too few data per one lock stripe (256 stripes in total)
 *   e.g too few elements in cache
 *   2. Test cases for eviction algorithms
 *   
 *   TODO:
 *   
 *   1. (++)Support for null values. It will allow to create Set
 *   2. Variable OFFSET (8)
 *   3. Fixed Key/Value sizes.
 *   4. (++) Add LZ4 compression algorithm
 *   5. Dynamic Cache resize: global and local.
 *   6. Range Queries with LevelDB persistence. 
 *   7. LevelDB persistence. 
 *   8. Cache Loaders (other than from local file system)
 *   9. (+, -)Item pinning, never expire. NOT TESTED YET
 *   10. Raw access (no serDe). Requires fixed sizes of Key and Value
 *       Raw access uses get(ByteBuffer ...), put(ByteBuffer ...), contains (ByteBuffer ...) etc.
 *   11. sun.misc.Unsafe for platforms which do not have native support.   
 */



/**
 * TODO:
 * 
 * 1) LIMIT mBucketNumber is int (max buckets is ~ 2G). Remove this limitation. It means that performance will start degrade
 * once we reach this limit (# objects)
 * 2) Get rid off all thread local buffers.
 * 3) ThreadLocalMalloc has memory leak?
 *
 * 
 * 7) Test getEvictedData
 *   
 * 11. TODO: Check excessive threads contention in PerfTest (threads=8)!!! This is small bucket issue
 *       With mBucketNumber = 200K definitely exists, with 2M - does not (?).
 * 12. TODO: getRandomKeys fails (CacheScanner) when Key class and Value class are set on a cache.
 */

/**
 * The Class OffHeapCache - Map.
 */
public class OffHeapCache {
	
	/** The not found. */
	public final long NOT_FOUND = -1;
	
	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(OffHeapCache.class);
	/** The Constant OFFSET. */
	public final static int OFFSET = 16;// 4 bytes for expiration; 4 bytes for eviction algo; 8 bytes for next ptr
	
	/** The Constant NP_OFFSET. */
	public final static int NP_OFFSET = 8;// Next ptr offset
	
	/** The MAX query processors. */
	private static int MAX_QUERY_PROCESSORS;
	
	/** The number of readers. */
	//private static int NUM_READERS;
	
	static{

		MAX_QUERY_PROCESSORS = 
			Integer.parseInt(System.getProperty("kode.max.query.processors", Runtime.getRuntime().availableProcessors()+""));
		//NUM_READERS= Integer.parseInt(System.getProperty("kode.max.concurrent.readers", "4"));
		LOG.debug("MaxQueryProcessors="+MAX_QUERY_PROCESSORS);
		//LOG.debug("MaxConcurrentReaders="+NUM_READERS);
	}
	
	/** The Constant NO_EXPIRE. */
	public final static int NO_EXPIRE = 0; // Can not expire	
	
	public final static int IMMORTAL = -1; // Can not expire, can't be evicted
	
	public final static int MAX_PUT_ATTEMPTS = 10;

	/** The number of buckets in the off-heap buffer. */
	protected int mBucketNumber = 1000000;// Default number
	
	/** The m bucket width. */
	private int mBucketWidth;
	
	/** The number of  lock stripes. */
	protected final static int LOCK_STRIPES = 256;// 8 bits
	
	/** The executor service. */
	private static ExecutorService sQueryExecutor = Executors.newFixedThreadPool(MAX_QUERY_PROCESSORS);
	
	/** The lock array. */
	private SpinReadWriteLock[] mLocks = new SpinReadWriteLock[LOCK_STRIPES];// default values;
	
	
	/** The memory pointer to off heap buffer. */
	/** This buffer is allocated with YAMM !!! */
	/** TODO Is it safe to allocate it with regular Unsafe?*/
	long mMemPointer;
	
	/** The current allocated memory size for cache. */
	AtomicLong mMemorySize = new AtomicLong(0);
	
	/** The total entries in a cache. */
	AtomicLong mTotalItems = new AtomicLong(0);
	/** Entry limit for cache. */
	private long mMaxEntries= 0;// 0 - unlimited
	
	/** Allocation memory limit for the cache. */
	private long mMaxMemory = 0;// 0 - unlimited
	
	/** Current global total allocated memory. */
	static AtomicLong sGlobalMemorySize = new AtomicLong(0);
	
	/** Global max memory for all caches. */
	static long sGlobalMaxMemory = 0;
	
	 /** When entries eviction starts. */
  private static float sHighWatermark = 0.99f;// 99% of sGlobalMaxMemory 
  
  /** Low watermark - when entries evictions stopped. */
  private static float sLowWatermark = 0.98f;//98% of sGlobalMaxMemory 
  
	/** Global eviction active */
	static AtomicBoolean sEvictionActive = new AtomicBoolean(false);
		
	/** Eviction stage: sets when memsize > HighWatermark & unsets when memsize < LowWatermark. */
	AtomicBoolean mEvictionActive = new AtomicBoolean(false);
	
	/** Eviction can be in Auto or Manual mode. It can be Manual even if it is Active*/
	AtomicBoolean mEvictionAuto = new AtomicBoolean(true);
	
	/** Cache is disabled. */
	AtomicBoolean mIsDisabled = new AtomicBoolean(false);
	
	/** When entries eviction starts. */
	private float mHighWatermark = 0.99f;// 99% of mMaxMemory or mMaxEntries
	
	/** Low watermark - when entries evictions stopped. */
	private float mLowWatermark = 0.95f;//95% of mMaxMemory or mMaxEntries
	
	/** cache eviction policy. */
	private EvictionPolicy mEvictionPolicy = EvictionPolicy.NONE;
	
	/** The actual eviction algorithm implementation. */
	private EvictionAlgo mEvictionAlgo;
	
	/** Eviction listener. */
	private EvictionListener mEvictionListener;
	
	/** Cache name. */
	private String mCacheName; 
	
	/** The m cache namespace. */
	private String mCacheNamespace;
	
	/** Default expiration timeout in seconds. */
	private int mExpireDefault = NO_EXPIRE; // no expiration
	
	/** Cache start time. */
	private long mStartTime; //
	
	/** Eviction candidate list - it gets updated on every put operation. */
	private int mEvCandidateListSize;	

	/** First eviction candidates - expired candidates. */
	private boolean mEvictOnExpireFirst = true;
		
	/** The m ser-deserializer buffer size. */
	private static int sSerDeBufferSize;
	
	/** The m ser de buffer. */
	private static ThreadLocal<ByteBufferWithAddress> sSerDeBufferTLS = 
							new ThreadLocal<ByteBufferWithAddress>();
	
	
	/** The m serializer. */
	protected SerDe mSerializer;
	
	/** The m cache config. */
	protected CacheConfiguration mCacheConfig;
	
	/** Compression stuff. */
	
	protected boolean mCompressionEnabled = false; 
	
	/** The m type. */
	protected CodecType mType = CodecType.NONE;	
	
	/** The m codec. */
	protected Codec mCodec;
	
	/** Persistent back store. */
	
	protected boolean mIsPersistent = false;
		
	/** The disk store. */
	protected DiskStore mDiskStore;
	
	/** The m persistence type. */
	protected PersistenceMode mPersistenceType;
	
	/** The Constant init_key_store_size. */
	private static final int init_key_store_size = 1024;
	/* Zero GC path - reuse local buffer to compare keys*/
	/** The Constant sKeyStoreTLS. */
	private final static ThreadLocal<byte[]> sKeyStoreTLS = new ThreadLocal<byte[]>(){
		@Override protected byte[] initialValue() {
            return new byte[init_key_store_size];
		}
	};
	
	/**
	 * Lock Free Mode. When we use cache in READ-ONLY mode
	 * or have one thread to do put/get operations
	 * Lock Free Mode is not safe and is recommended only
	 * in the following scenarios:
	 * 
	 * 		1. MT env.Open Cache, load data, set Lock Free Mode, then enable cache and use as READ ONLY cache
	 *         
	 *      2. ST env. Open Cache, Laod data, set LOCK FREE and use
	 *      
	 *      There are no Read/Write mempry barriers (for perf reasons), taht is why
	 *      modifying this variable in MT env is not safe.  
	 */
	private boolean mLockFreeMode = false; 
		
	/** Cache can store any objects, any keys, any values but if keyClass and valueClass are defined we can reduce serialized footprint by 8 bytes. */
	private Class<?> mKeyClass;
	
	/** The m value class. */
	private Class<?> mValueClass;
	
	/** Use this one during forced eviction 0<= mForcedEvictionCurrentStripe< LOCK_STRIPES. */
	private AtomicLong mForcedEvictionCounter=new AtomicLong(-1);
	
	
	/** Cache statistics. */
	private AtomicLong mHitCount = new AtomicLong(0);
	
	/** Failed put . This failure is possible when, memory allocator fails due to heap fragmentation*/
	
	private AtomicLong mPutFailed = new AtomicLong(0);
	
	/** The total request count. */
	private AtomicLong mTotalCount = new AtomicLong(0);
	
	/** Keeps object histogram according to eviction algorithm */
	private AtomicReference<NumericHistogram> mHistogram = new AtomicReference<NumericHistogram>();
	
	private int mHistogramBins = 100;
	
	private int mHistogramSamples = 2000;
	
	/**TODO reduce default update interval to 1 sec*/
	private long mHistogramInterval = 5000; //  5 sec
	
	private boolean mHistogramLogEnabled = false;
	
	private HistogramUpdater mUpdater;
	
	private long lastHistogramUpdateTime;
	
	private List<StorageOptimizer> storageOptimizerList;
	
	/**
	 * Updates object histogram periodically
	 * @author vrodionov
	 *
	 */
	class HistogramUpdater extends Thread{
		
		public HistogramUpdater(){
			super("HistogramUpdater");		
			setDaemon(true);
		}
		
		public void run(){
			
			try{
				if(isRealEvictionPolicy() == false){
					LOG.warn("[HistogramUpdater] Eviction is disabled. Exiting HistogramUpdater.");
					return;
				}
				while(true){
					updateHistogram();
					lastHistogramUpdateTime = System.currentTimeMillis();
					try {
						Thread.sleep(OffHeapCache.this.mHistogramInterval);
					} catch (InterruptedException e) {}				
				}
			} finally{
				LOG.fatal(Thread.currentThread().getName() +" died.");
			}
			
		}

		private void updateHistogram() {
			try {
				Object[] keys = OffHeapCache.this.getRandomKeys(OffHeapCache.this.mHistogramSamples);
				int i =0;
				int count = 0;
				
				NumericHistogram hist = new NumericHistogram();
				hist.allocate(OffHeapCache.this.mHistogramBins);
				
				for(; i < keys.length; i++){
					if( keys[i] == null) break;
					long evictionData = OffHeapCache.this.getEvictionData(keys[i]);
					if( evictionData >=0){						
						hist.add((double) evictionData);
						count++;
					}
					
				}
				
				OffHeapCache.this.mHistogram.set(hist);
				
				if(OffHeapCache.this.mHistogramLogEnabled){
					LOG.info("[HistogramUpdater] Total updated:"+i+" samples [count="+count+"]");
					dumpHistogram(hist);
				}
			} catch (Exception e) {
				LOG.error(e);
				e.printStackTrace();
			} 
			
		}

		private void dumpHistogram(NumericHistogram hist) {
			double[] quantiles = new double[]{0,0.05, 0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.};
			double[] values = hist.quantiles(quantiles);
			
			for(int i=0; i < values.length; i++){
				LOG.info(quantiles[i]+"="+(long)values[i]);
			}
			LOG.info("Max age estimate: "+(long)(values[values.length-1] - values[0])+"ms");
			
		}
	}
	
  class StorageOptimizer extends Thread {
    OptimizerPolicy policy;
    OffHeapCache cache;
    long recompressed = 0;
    long skipped = 0;
    long lastStatsTime = 0;
    long lastRecompressed = 0;
    long lastRecompressed2 = 0;
    long lastSkipped = 0;
    long lastSkipped2 = 0;
    long statInterval = 1000;
    long sleepIntervalMin= 0; // zero
    long sleepIntervalMax = 2; // 2 ms
    
    int startIndex; // inclusive
    int endIndex;   // inclusive

    public StorageOptimizer() {
      super("StorageOptimizer");
      this.policy = new DefaultOptimizerPolicy();
      setDaemon(true);
      setPriority(Thread.MIN_PRIORITY);
      cache = OffHeapCache.this;
      startIndex = 0;
      endIndex = cache.getTotalBuckets() -1;
    }

    public StorageOptimizer(OptimizerPolicy policy) {
      super("StorageOptimizer");
      this.policy = policy;
      setDaemon(true);
      setPriority(Thread.MIN_PRIORITY);
      cache = OffHeapCache.this;
      startIndex = 0;
      endIndex = cache.getTotalBuckets() -1;
    }
    public StorageOptimizer(int startIndex, int endIndex) {
      super("StorageOptimizer");
      this.policy = new DefaultOptimizerPolicy();
      setDaemon(true);
      setPriority(Thread.MIN_PRIORITY);
      cache = OffHeapCache.this;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    public StorageOptimizer(OptimizerPolicy policy, int startIndex, int endIndex ) {
      super("StorageOptimizer");
      this.policy = policy;
      setDaemon(true);
      setPriority(Thread.MIN_PRIORITY);
      cache = OffHeapCache.this;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }
    
    public void run() {
      
      int current = startIndex;
      long sleepInterval = sleepIntervalMin;
      
      // Wait until histogram is enabled if policy is not constant
      while (policy.isConstant() == false && cache.mHistogram.get() == null) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
      lastStatsTime = System.currentTimeMillis();

      for (;;) {
        NumericHistogram hist = cache.mHistogram.get();
        if (current == endIndex +1) {
          current =  startIndex;
        }
        CacheScanner scanner = cache.getRangeScanner(current, current);
        scanner.setOperationMode(CacheScanner.Mode.UPDATE);

        while (scanner.hasNext()) {
          long ptr = scanner.nextPointer();
          if (ptr != 0L) {
            // Get eviction data
            long evData = IOUtils.getUInt(ptr, 4);
            evData = mEvictionAlgo.translate(evData);
            CodecType type = cache.getCodecTypeForRecord(ptr);
            
            Codec newCodec = policy.getCodec(hist, evData);
            //LOG.info(newCodec.getType());
            
            if (newCodec.getType() != type) {
              // Recompress
              try {
                Object key = scanner.key(ptr);
                Object value = scanner.value(ptr);
                cache.put(key, value, newCodec);
                recompressed++;
              } catch (Exception e) {
                LOG.fatal("fatal", e);
                // terminate thread
                return;
              }
            } else{
              skipped++;
            }
          }
        }

        scanner.close();
        current++;
        try {
          Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block

        }
        // do stats
        if (System.currentTimeMillis() - lastStatsTime > statInterval) {
          if(lastRecompressed == recompressed) {
            // Set some non-zero sleep interval if
            // no changes were detected during last interval
            sleepInterval = sleepIntervalMax;
          } else{
            // Kick of aggressive looping
            sleepInterval = sleepIntervalMin;
          }
          // This is debug
          //LOG.info("Recompressed=" + recompressed + " skipped=" + skipped);
          lastStatsTime = System.currentTimeMillis();
          lastRecompressed2 = lastRecompressed;
          lastRecompressed = recompressed;
          lastSkipped2 = lastSkipped;
          lastSkipped = skipped;
          
        }
      }
    }
    
    public double getEstimatedCompressionRatio()
    {
 
       Codec[] codecs  = policy.getAllCodecs();
       long totalProcessed = 0;
       for(int i=0;  i< codecs.length; i++ ){
         totalProcessed += codecs[i].getTotalProcessed();
       }
       if(totalProcessed == 0 ) return mCodec.getAvgCompressionRatio();
       
       // Calculate avg compression for Optimizer
       double optimizerCompRatio = 0;
       for(int i=0; i < codecs.length; i++){
         long total = codecs[i].getTotalProcessed();
         optimizerCompRatio += (((double) total) / totalProcessed ) * codecs[i].getAvgCompressionRatio(); 
       }
       
       return optimizerCompRatio ;
       
    }
    
    public double getEstimatedRecompressedRatio()
    {
      if(recompressed == 0) return 0;
      long lastPeriodRecompressed = lastRecompressed - lastRecompressed2;
      long lastPeriodSkipped = lastSkipped - lastSkipped2;
      if(lastPeriodSkipped == 0) return 0;
      if(lastPeriodRecompressed == 0) return 1;
      return (double) lastPeriodSkipped / (lastPeriodSkipped + lastPeriodRecompressed);
    }
  }
	
	interface OptimizerPolicy
	{
	  /**
	   *  Percentile 0 -1.0
	   *  0 - means the object is the most UNUSED in cache
	   *  1 - meand the object is the most used in cache
	   *  
	   * @param percentile
	   * @return
	   */
	  public Codec getCodec(NumericHistogram hist, double evictionData);
	  
	  public boolean isConstant();
	  
	  public Codec[] getAllCodecs();
	    
	  
	}
	/**
	 * Default optimizer re-compresses all to LZ4HC
	 * @author vrodionov
	 *
	 */
	class DefaultOptimizerPolicy implements OptimizerPolicy
	{

	  Codec codec = new LZ4HCCodec();
	  
    @Override
    public Codec getCodec(NumericHistogram hist, double evictionData) {
      return codec;
    }

    @Override
    public boolean isConstant() {
      return true;
    }

    @Override
    public Codec[] getAllCodecs() {
      return new Codec[]{codec};
    }
	  	  
	}

  class SplitOptimizerPolicy implements OptimizerPolicy
  {

    Codec codec1 = new LZ4HCCodec();
    Codec codec2 = new DeflateCodec();
    double split = 0.5d;
    
    public SplitOptimizerPolicy()
    {
      
    }
    public SplitOptimizerPolicy(double split)
    {
     this.split = split; 
    }
    
    @Override
    public Codec getCodec(NumericHistogram hist, double evictionData) {
      double val = hist.quantile(split);
      if(val > evictionData) return codec2;
      return codec1;
    }
    @Override
    public boolean isConstant() {
      return false;
    }
    @Override
    public Codec[] getAllCodecs() {     
      return new Codec[]{codec1, codec2};
    }
        
  }
	
  public double getEstimatedCompressionRatio()
  {
    if(isOptimizerEnabled() == false){ // not enabled
      if (isCompressionEnabled()){
        return mCodec.getAvgCompressionRatio();
      } else{
        return 1.;
      }
    } else { // enabled
      double recompressedRatio = storageOptimizerList.get(0).getEstimatedRecompressedRatio();
      double estCompRatio = storageOptimizerList.get(0).getEstimatedCompressionRatio();
      double compRatio = isCompressionEnabled() ? mCodec.getAvgCompressionRatio() : 1.;
      return recompressedRatio * estCompRatio + compRatio * ( 1 - recompressedRatio); 
    }
  }
	
  public boolean isOptimizerEnabled()
  {
    return storageOptimizerList != null && storageOptimizerList.size() > 0;
  }
  
  public boolean isCompressionEnabled(){
    return mCodec != null && mCodec.getType() != CodecType.NONE;
  }
  
	public long getLastHistogramUpdateTime()
	{
	  return lastHistogramUpdateTime;
	}
	
	public NumericHistogram getObjectHistogram()
	{
		return mHistogram.get();
	}	
	
//	public static long getLastPutAddress()
//	{
//		return sLastPutAddress.get();
//	}
	
	/**
	 * Set the last put address (thread-local storage)
	 * @param address
	 */
//	public static void setLastPutAddress(long address)
//	{
//		sLastPutAddress.set(address);
//	}
	
	
	/**
	 * Get Max memory allowed for all caches.
	 *
	 * @return max memory
	 */
	public static long getGlobalMaxMemorySize()
	{
		return sGlobalMaxMemory;
	}
	
	/**
	 * Gets the global memory size.
	 *
	 * @return the global memory size
	 */
	public static long getGlobalMemorySize()
	{
		return sGlobalMemorySize.get();
	}
	
	/**
	 * Checks if is lock free mode.
	 *
	 * @return true, if is lock free mode
	 */
	public final boolean isLockFreeMode() {return mLockFreeMode;}
	
	/**
	 * Checks if is not lock free mode.
	 *
	 * @return true, if is not lock free mode
	 */
	public final boolean isNotLockFreeMode() {return !mLockFreeMode;}
	
	/**
	 * Sets the lock free mode.
	 *
	 * @param v the new lock free mode
	 */
	public final void setLockFreeMode(boolean v)
	{
		mLockFreeMode = v;
	}
	
	/**
	 * Gets the compression codec.
	 *
	 * @return the compression codec
	 */
	public final Codec getCompressionCodec()
	{
		return mCodec;
	}
	
	/**
	 * Gets the hit count.
	 *
	 * @return the hit count
	 */
	public long getHitCount()
	{
		return mHitCount.get();
	}
	
	/**
	 * Gets the total count.
	 *
	 * @return the total count
	 */
	public long getTotalRequestCount()
	{
		return mTotalCount.get();
	}
	
	
	public long getFailedPuts()
	{
	   return mPutFailed.get();
	}
		
	
	/**
	 * Gets the evicted count.
	 *
	 * @return the evicted count
	 */
	public long getEvictedCount()
	{
		if(mEvictionAlgo != null){
			return mEvictionAlgo.getTotalEvictedItems();
		}
		return 0;
	}
	
	/**
	 * Gets the local buffer.
	 *
	 * @return the local buffer
	 * @throws NativeMemoryException the native memory exception
	 */
	public final ByteBufferWithAddress getLocalBufferWithAddress() 
	{
		ByteBufferWithAddress buf = OffHeapCache.sSerDeBufferTLS.get();
		if( buf == null){
			ByteBuffer bbuf = NativeMemory.allocateDirectBuffer(64, sSerDeBufferSize);
			long ptr = NativeMemory.getBufferAddress(bbuf);
			buf = new ByteBufferWithAddress(bbuf, ptr);
			OffHeapCache.sSerDeBufferTLS.set(buf);
		}
		return buf;
	}
	
	/**
	 * The Class ByteBufferWithAddress.
	 */
	public class ByteBufferWithAddress
	{
		
		/** The buffer. */
		private ByteBuffer buffer;
		
		/** The address. */
		long address;
		
		/**
		 * Instantiates a new byte buffer with address.
		 *
		 * @param buf the buf
		 * @param address the address
		 */
		public ByteBufferWithAddress(ByteBuffer buf, long address) {
			this.buffer = buf;
			this.address = address;
		}
		
		/**
		 * Gets the buffer.
		 *
		 * @return the buffer
		 */
		public final ByteBuffer getBuffer(){ return buffer;}
		
		/**
		 * Gets the address.
		 *
		 * @return the address
		 */
		public final long getAddress(){return address;}
	}
	/**
	 * Gets the ser de.
	 *
	 * @return the ser de
	 */
	final public SerDe getSerDe()
	{
		return mSerializer;
	}
	
	/**
	 * Sets the eviction listener.
	 *
	 * @param l the new eviction listener
	 */
	public void setEvictionListener(EvictionListener l)
	{
		this.mEvictionListener = l;
	}
	
	/**
	 * Gets the eviction listener.
	 *
	 * @return the eviction listener
	 */
	public EvictionListener getEvictionListener()
	{
		return mEvictionListener;
	}
	
	/**
	 * Sets the evict on expire first policy.
	 *
	 * @param value the new evict on expire first
	 */
	public void setEvictOnExpireFirst(boolean value)
	{
		this.mEvictOnExpireFirst = value;
		if(mEvictionAlgo != null){
			mEvictionAlgo.setEvictOnExpireFirst(value);
		}
	}
	
	/**
	 * Gets the evict on expire first.
	 *
	 * @return the evict on expire first
	 */
	public boolean getEvictOnExpireFirst()
	{
		return mEvictOnExpireFirst;
	}
	
	/**
	 * Gets the cache eviction policy.
	 *
	 * @return the cache eviction policy
	 */
	public EvictionPolicy getCacheEvictionPolicy() {
		return mEvictionPolicy;
	}

	/**
	 * Is real eviction?.
	 *
	 * @return true, if is real eviction policy
	 */
	public final boolean isRealEvictionPolicy()
	{
		return mEvictionPolicy != EvictionPolicy.NONE;
	}
	
	/**
	 * Sets the cache eviction policy.
	 *
	 * @param mEvictionPolicy the new cache eviction policy
	 */
	public void setCacheEvictionPolicy(EvictionPolicy mEvictionPolicy) {
		this.mEvictionPolicy = mEvictionPolicy;
		switch(mEvictionPolicy){
			case NONE: mEvictionAlgo = new EvictionNone(this); break;
			case LRU: mEvictionAlgo = new EvictionLRU(this); break;
			case LFU: mEvictionAlgo = new EvictionLFU(this); break;
			case FIFO: mEvictionAlgo = new EvictionFIFO(this); break;
			case RANDOM: mEvictionAlgo = new EvictionRandom(this); break;
			case LRU2Q: mEvictionAlgo = new EvictionLRU2Q(this); break;
		}
	}

	/**
	 * Gets the cache name.
	 *
	 * @return the cache name
	 */
	public String getCacheName() {
		return mCacheName;
	}

	/**
	 * Sets the cache name.
	 *
	 * @param mCacheName the new cache name
	 */
	public void setCacheName(String mCacheName) {
		this.mCacheName = mCacheName;
	}

	
	/**
	 * Gets the cache namespace.
	 *
	 * @return the cache namespace
	 */
	public String getCacheNamespace()
	{
		return mCacheNamespace;
	}
	
	
	/**
	 * Sets the cache namespace.
	 *
	 * @param ns the new cache namespace
	 */
	public void setCacheNamespace(String ns)
	{
		this.mCacheNamespace = ns;
	}
	/**
	 * Gets the default expiration timeout.
	 *
	 * @return the default expiration timeout
	 */
	public int getDefaultExpirationTimeout() {
		return mExpireDefault;
	}

	/**
	 * Sets the default expiration timeout.
	 *
	 * @param mExpireDefault the new default expiration timeout
	 */
	public void setDefaultExpirationTimeout(int mExpireDefault) {
		this.mExpireDefault = mExpireDefault;
	}

	/**
	 * Gets the cache start time.
	 *
	 * @return the cache start time
	 */
	public long getCacheStartTime() {
		return mStartTime;
	}

	
	/**
	 * Gets the entries limit.
	 *
	 * @return the entries limit
	 */
	public long getEntriesLimit() {
		return mMaxEntries;
	}

	/**
	 * Sets the entries limit.
	 *
	 * @param mMaxEntries the new entries limit
	 */
	public void setEntriesLimit(int mMaxEntries) {
		this.mMaxEntries = mMaxEntries;
	}

	/**
	 * Gets the memory limit.
	 *
	 * @return the memory limit
	 */
	public long getMemoryLimit() {
		return mMaxMemory;
	}

	/**
	 * Sets the memory limit.
	 *
	 * @param mMaxMemory the new memory limit
	 * @deprecated
	 */
	public void setMemoryLimit(long mMaxMemory) {
		this.mMaxMemory = mMaxMemory;
	}

	/**
	 * Gets the high watermark.
	 *
	 * @return the high watermark
	 */
	public float getHighWatermark() {
		return mHighWatermark;
	}

	/**
	 * Sets the high watermark.
	 *
	 * @param mHighWatermark the new high watermark
	 * @deprecated
	 */
	public void setHighWatermark(float mHighWatermark) {
		this.mHighWatermark = mHighWatermark;
	}

	/**
	 * Gets the low watermark.
	 *
	 * @return the low watermark
	 */
	public float getLowWatermark() {
		return mLowWatermark;
	}

	/**
	 * Sets the low watermark.
	 *
	 * @param mLowWatermark the new low watermark
	 * @deprecated
	 */
	public void setLowWatermark(float mLowWatermark) {
		this.mLowWatermark = mLowWatermark;
	}

	/**
	 * Gets the total buckets.
	 *
	 * @return the total buckets
	 */
	public int getTotalBuckets() {
		return mBucketNumber;
	}

	
	/**
	 * Gets the lock stripes count.
	 *
	 * @return the lock stripes count
	 */
	public static int getLockStripesCount()
	{
		return LOCK_STRIPES;
	}
	
	
	/**
	 * Gets the epoch start time.
	 *
	 * @return the epoch start time
	 */
	public long getEpochStartTime()
	{
		return mStartTime;
	}
	
	/**
	 * Instantiates a new off heap cache.
	 *
	 * @param ns the ns
	 * @param name the name
	 * @param buckets the buckets
	 * @throws NativeMemoryException the j emalloc exception
	 */
	OffHeapCache(String ns, String name, int buckets) throws NativeMemoryException
	{
		this(buckets);
		this.mCacheNamespace = ns;
		this.mCacheName = name;
	}
	
	
	/**
	 * Instantiates a new off heap cache.
	 *
	 * @param config the config
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	OffHeapCache(CacheConfiguration config) throws NativeMemoryException, IOException
	{
		this(config.getCacheNamespace(), config.getCacheName(), config.getBucketNumber());
		this.mCacheConfig = config;
		this.mMaxMemory = config.getMaxMemory();
		//if( this.mMaxMemory )
		if (sGlobalMaxMemory == 0){
			// Set global memory
			sGlobalMaxMemory = config.getMaxGlobalMemory();
		}
		// Increment global allocated memory by bucketNumber* 8 (64-byte mode)
		sGlobalMemorySize.addAndGet((((long)8)*mBucketNumber));
		// Allocated already
		//mMemorySize.addAndGet((((long)8)*mBucketNumber));
		
		this.mEvictionAlgo = getEvictionAlgo(config.getEvictionPolicy());
		//this.mEvictionAttempts = config.getEvictionAttempts();
		
		this.mEvictOnExpireFirst = config.isEvictOnExpireFirst();
		this.mExpireDefault = config.getDefaultExpireTimeout();
		this.mHighWatermark  = config.getHighWatermark();
		this.mLowWatermark = config.getLowWatermark();
		this.mMaxEntries = config.getMaxEntries();
		this.mEvCandidateListSize = config.getCandidateListSize();

		
		try{
			this.mKeyClass = config.getKeyClassName().equals(Defaults.DEFAULT_NONE)? null: 
						Class.forName(config.getKeyClassName());
		
			this.mValueClass = config.getValueClassName().equals(Defaults.DEFAULT_NONE)? null: 
						Class.forName(config.getKeyClassName());	;
		}catch(ClassNotFoundException e){
			throw new IOException(e);
		}
						// This one is should be global
		OffHeapCache.sSerDeBufferSize = config.getSerDeBufferSize();
				
		//TODO - this part of a global configuration
		System.setProperty(SerDe.SERDE_BUF_SIZE, Integer.toString(config.getSerDeBufferSize()));
		System.setProperty(Codec.COMPRESSION_THRESHOLD, Integer.toString(config.getCompressionThreshold()));
		this.mSerializer = SerDe.getInstance();
		
		this.mCompressionEnabled = config.isCompressionEnabled();
		
		// This return separate instance
		this.mCodec = CodecFactory.getInstance().getCodec(config.getCodecType());
		
		if(mCodec != null){
			this.mCodec.setCompressionThreshold(config.getCompressionThreshold());
			int level = config.getCompressionLevel();
			if(level > 0){
				this.mCodec.setLevel(0);
			}
		}
		// Initializes histogram stuff: we need it for advanced eviction policies
		// and to support GC in file-based storage
		if( config.isHistogramEnabled() && isRealEvictionPolicy() || 
		    config.getEvictionPolicy().equals(EvictionPolicy.LRU2Q.toString()) || config.isOptimizerEnabled()){
		  
		  if(config.isHistogramEnabled() == false){
		    config.setHistogramEnabled(true);
		    if( config.isOptimizerEnabled() == false){
		      LOG.warn("Object histogram must be enabled when Eviction="+EvictionPolicy.LRU2Q);
		    } else{
          LOG.warn("Object histogram must be enabled when RAM storage optimization is enabled");

		    }
		  }
		  
			this.mHistogramBins = config.getHistogramBins();
			this.mHistogramInterval = config.getHistogramUpdateInterval();
			this.mHistogramSamples  = config.getHistogramSamples();
			this.mHistogramLogEnabled = config.isHistogramLogEnabled();
			
			mUpdater = new HistogramUpdater();
			mUpdater.start();
			
		}
		
		// Update global memory allocator
		// max memory limit. every off heap cache instance adds
		// its own max memory.
		Malloc malloc = NativeMemory.getMalloc();
		synchronized(malloc){
		  long max = malloc.getMaxMemorySize();
		  // We subtract memory already allocated
		  //FIXME: max + mMaxMemory - (((long)8)*mBucketNumber) < 0
		  malloc.setMaxMemorySize(max + mMaxMemory - (((long)8)*mBucketNumber));
		}
		// Start storage optimizer
		if(config.isOptimizerEnabled()){
		  int numThreads = config.getOptimizerThreads();
		  int optimizerLevel = config.getOptimizerLevel();
		  if(optimizerLevel < 1 || optimizerLevel > 5){
		    LOG.warn("Wrong optimizer level: "+optimizerLevel);
		  } else{
		    LOG.info("Starting "+ numThreads + " of RAM storage optimizers. Level="+optimizerLevel);
		      storageOptimizerList = 
		      new ArrayList<StorageOptimizer>();
		      int startIndex =0;
		      int endIndex = 0;
		  
		      for(int i=0; i < numThreads; i++){		    
		        startIndex = i * (mBucketNumber/ numThreads);
		        endIndex = ((i+1) * mBucketNumber)/(numThreads) - 1;
		        OptimizerPolicy policy = getPolicyForLevel(optimizerLevel);
		        StorageOptimizer storageOptimizer = new StorageOptimizer(policy, startIndex, endIndex);
		        storageOptimizer.start();
		        storageOptimizerList.add(storageOptimizer);
		      
		      }
		  }
		}	
	}
	

  private OptimizerPolicy getPolicyForLevel(int optimizerLevel) {
    switch(optimizerLevel){
      case 1: return new DefaultOptimizerPolicy();
      case 2: return new SplitOptimizerPolicy(0.35d);
      case 3: return new SplitOptimizerPolicy (0.50d);
      case 4: return new SplitOptimizerPolicy (0.75d);
      case 5: return new SplitOptimizerPolicy(1.d);
    }
    return null;
  }

  /**
	 * Instantiates a new off heap cache.
	 *
	 * @param config the config
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public OffHeapCache(ExtCacheConfiguration config) throws NativeMemoryException, IOException
	{
		this(config.getCacheConfig());
		//TODO ?
		this.mIsDisabled = new AtomicBoolean(config.isDisabled());
		// TODO ?
		this.mStartTime = config.getEpochTime();
		this.mMemorySize = new AtomicLong(config.getMemorySize());
		this.mTotalItems = new AtomicLong(config.getTotalItems());
		this.mEvictionActive = new AtomicBoolean(config.isEvictionActive());

		sGlobalMemorySize.addAndGet(mMemorySize.get());
		
	}
	
	
	/**
	 * Checks if is key class defined.
	 *
	 * @return true, if is key class defined
	 */
	private final boolean isKeyClassDefined()
	{
		return mKeyClass != null;
	}
	
	/**
	 * Gets the key class.
	 *
	 * @return the key class
	 */
	private final Class<?> getKeyClass()
	{
		return mKeyClass;
	}
	
	/**
	 * Checks if is value class defined.
	 *
	 * @return true, if is value class defined
	 */
	private final boolean isValueClassDefined()
	{
		return mValueClass != null;
	}
	
	/**
	 * Gets the value class.
	 *
	 * @return the value class
	 */
	private final Class<?> getValueClass()
	{
		return mValueClass;
	}
	
	
	/**
	 * Gets the cache configuration.
	 *
	 * @return the cache configuration
	 */
	public CacheConfiguration getCacheConfiguration()
	{
		return mCacheConfig;
	}
	
	 /**
 	 * Gets the ext cache configuration.
 	 *
 	 * @return the ext cache configuration
 	 */
 	public ExtCacheConfiguration getExtCacheConfiguration()
	 {
		 ExtCacheConfiguration cfg = new ExtCacheConfiguration();
		 cfg.setCacheConfig(mCacheConfig);
		 cfg.setDisabled(mIsDisabled.get());
		 cfg.setEpochTime(mStartTime);
		 cfg.setEvictionActive(mEvictionActive.get());
		 cfg.setMemorySize(mMemorySize.get());
		 cfg.setTotalItems(mTotalItems.get());
		 return cfg;
	 }
	
	/**
	 * Gets the eviction algo.
	 *
	 * @param policyName the policy name
	 * @return the eviction algo
	 */
	private EvictionAlgo getEvictionAlgo(String policyName)
	{
		if(policyName.equalsIgnoreCase("NONE")) {
			mEvictionPolicy = EvictionPolicy.NONE;
			return new EvictionNone(this);
		}
		if(policyName.equalsIgnoreCase("LRU")){
			mEvictionPolicy = EvictionPolicy.LRU;
			return new EvictionLRU(this);
		}
		if(policyName.equalsIgnoreCase("LFU")) {
			mEvictionPolicy = EvictionPolicy.LFU;
			return new EvictionLFU(this);
		}
		if(policyName.equalsIgnoreCase("FIFO")) {
			mEvictionPolicy = EvictionPolicy.FIFO;
			return new EvictionFIFO(this);
		} 
		if(policyName.equalsIgnoreCase("RANDOM")) {
			mEvictionPolicy = EvictionPolicy.RANDOM;
			return new EvictionRandom(this);
		} 
    if(policyName.equalsIgnoreCase("LRU2Q")){
      mEvictionPolicy = EvictionPolicy.LRU2Q;
      return new EvictionLRU2Q(this);
    }
		return null;
	}
	
	/**
	 * Instantiates a new off heap hash map.
	 *
	 * @param buckets the buckets
	 * @throws NativeMemoryException the j emalloc exception
	 * 
	 * TODO: FIX malloc for 2GB +
	 */
	OffHeapCache( int buckets) 
	{
		this.mBucketNumber = 
			(buckets/LOCK_STRIPES)*LOCK_STRIPES == buckets? buckets: ((buckets/LOCK_STRIPES)+1)*LOCK_STRIPES;
		

		this.mBucketWidth = mBucketNumber/ LOCK_STRIPES;

		mMemPointer = NativeMemory.getUnsafe().allocateMemory((long) mBucketNumber * 8);//NativeMemory.malloc((long) mBucketNumber * 8);
				
		// Nullify all - not very efficient
		// Nullify memory
		for(int i=0; i < mBucketNumber; i++)
		{
			IOUtils.putLong( mMemPointer, ((long)(i)) * 8, 0);
		}
		
		for(int i=0; i < LOCK_STRIPES; i++)
		{
			mLocks[i] = new SpinReadWriteLock();
		}
		//This is epoch start time
		mStartTime = System.currentTimeMillis();
				
		// This is default algo - NONE
		mEvictionAlgo = new EvictionNone(this);
	}

	/**
	 * Gets the buffer address.
	 *
	 * @return the buffer address
	 */
	public long getBufferAddress()
	{
		return mMemPointer;
	}
	
	/**
	 * Allocate main buffer.
	 *
	 * @param num the num
	 * @return the long buffer
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected LongBuffer allocateMainBuffer(int num) throws NativeMemoryException
	{
		ByteBuffer buf = NativeMemory.allocateDirectBuffer(256, num*8);
		return buf.asLongBuffer();
	}
	
	/**
	 * Gets the mem pointer.
	 *
	 * @return the mem pointer
	 */
	public long getMemPointer() {
		return mMemPointer;
	}

	/**
	 * Gets the eviction algo.
	 *
	 * @return the eviction algo
	 */
	public EvictionAlgo getEvictionAlgo()
	{
		return mEvictionAlgo;
	}
	
	
	/**
	 * Gets the lock.
	 *
	 * @param index the index
	 * @return the lock
	 */
	public final SpinReadWriteLock getLock(final long index)
	{
		if(isLockFreeMode()) return null;
		return mLocks[(int)(index/mBucketWidth)];
	}
	
	/**
	 * Gets the lock index.
	 *
	 * @param index the index
	 * @return the lock index
	 */
	final int getLockIndex(final long index)
	{
		return (int)(index/mBucketWidth);
	}
	
	/**
	 * TODO - implement cache reset.
	 * Set last 
	 */
	public void reset()
	{
		
	}
	
	
	/**
	 * Checks if is expired entry.
	 *
	 * @param ptr the ptr
	 * @return true, if is expired entry
	 */
	public final boolean isExpiredEntry( long ptr) {
		long start = getEpochStartTime();
		long secs = IOUtils.getUInt(ptr, 0);
		if(secs == NO_EXPIRE) return false;
		return System.currentTimeMillis() > (start + 1000*secs);
	}
	
	/**
	 * Clear the cache (not optimized yet).
	 * We need fast implementation for TEMP caches
	 * when caches are created and destroyed on the fly
	 * Unfortunately, NativeMemory.free is a blocking syncronized call
	 * 
	 * Optimize: 
	 * 1. Global lock for cache. When clear starts all cache read/write operations
	 *    must throw exceptions
	 * 2. Do clear region based (for multi threading)
	 * 3. implement it in a native code
	 * 4. scan   
	 * 
	 * TODO: optimize clear
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void clear() throws NativeMemoryException {
		int index = 0;
		ReentrantReadWriteLock lock = getLock(0);
		
		WriteLock writeLock = null;
		
		if(lock != null){
			writeLock = lock.writeLock();
			writeLock.lock();
		}
		
		try {
			for (; index < mBucketNumber; index++) {
				// Get Bucket Read Lock
				ReentrantReadWriteLock newlock = getLock(index);
				
				if (newlock != null && newlock != lock) {
					writeLock.unlock();
					lock = newlock;
					writeLock = lock.writeLock();
					writeLock.lock();
				}

				long ptr = IOUtils.getLong(mMemPointer, ((long)index) * 8);
				if (ptr == 0) {
					continue;

				} else {
					freePtr(ptr);
				}
			}
		} finally {
			if(writeLock != null) writeLock.unlock();
		}

	}

	/**
	 * Recursive method. - Optimize it (it is used only in clear())
	 *
	 * @param ptr the ptr
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected void freePtr(long ptr) throws NativeMemoryException
	{
		if(ptr == 0) return;
		long nextPtr = getNextAddress(ptr);
		ptr = getRealAddress(ptr);
		long size = NativeMemory.mallocUsableSize(ptr);
		NativeMemory.free(ptr);
		mTotalItems.decrementAndGet();	
		updateAllocatedMemory(-size);
		freePtr(nextPtr);		
	}
	
	/**
	 * This is for bg GC thread direct access.
	 *
	 * @return the locks
	 */
	
	public SpinReadWriteLock[] getLocks()
	{
		return mLocks;
	}

	
	

	public boolean contains(Object key) throws IOException
	{
		return containsKey(key);
	}
	
	/**
	 * Contains key.
	 *
	 * @param key the key
	 * @return true, if successful
	 * @throws IOException 
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean containsKey(Object key) throws IOException 
	{
		
		ByteBufferWithAddress bufa = getLocalBufferWithAddress();
		ByteBuffer buf = bufa.getBuffer();
		buf.clear();

		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		} else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);
		
		return containsKey(buf);
		
	}
	
	/**
	 * Contains key. First 4 bytes of ByteBuffer - length
	 * of key in bytes followed by key bytes 
	 *
	 * @param key the key
	 * @return true, if successful
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean containsKey(ByteBuffer key) 
	{		
		
		long bufPtr = NativeMemory.getBufferAddress(key);
		long ptr = getNativePtr(key, bufPtr, true);
		return ptr != 0;
	}
	
	/**
	 * More optimal  
	 */
	
	/**
	 * Sets the first bit.
	 *
	 * @param ptr the new first bit
	 */
	final void setFirstBit(final long ptr)
	{
		short v  = IOUtils.getUByte(ptr, 0);
		IOUtils.putUByte(ptr, 0, (short)(v | (short) 0x8000));
	}
	
	/**
	 * Unset first bit.
	 *
	 * @param ptr the ptr
	 * @return real memory pointer
	 */
	public static final long getRealAddress(final long ptr)
	{
		return ptr & 0x7fffffffffffffffL;
	}
	
	/**
	 * Checks if is first bit.
	 *
	 * @param ptr the ptr
	 * @return true, if is first bit
	 */
	final boolean isFirstBit(final long ptr)
	{
		return (IOUtils.getUByte(ptr, 0) & 0x8000) != 0;
	}
	
	/**
	 * Child has children.
	 *
	 * @param ptr the ptr
	 * @return true, if successful
	 */
	final boolean childHasChildren(long ptr)
	{
		return (IOUtils.getUByte(ptr+8, 0) & 0x8000) != 0;
	}
	
	/**
	 * This method is only for internal testing.
	 *
	 * @param number the number
	 * @param total the total
	 * @return the scanner
	 */
	
	public CacheScanner getScanner(int number, int total)
	{
		return new CacheScanner(this, number, total);
	}
	
	/**
	 * This range scanner is used to get pseudo-random sample of records
	 * @param startIndex
	 * @param endIndex
	 * @return
	 */
	public CacheScanner getRangeScanner(int startIndex, int endIndex)
	{
		return new CacheScanner(this, startIndex, endIndex, 0);
	}

	/**
	 * Prefetch.
	 *
	 * @param key the key
	 * @param bufPtr the buf ptr
	 * @return the int
	 */
	protected int prefetch(ByteBuffer key, long bufPtr)
	{
		key.position(0);
		int len = key.getInt();
		int index = Math.abs(Utils.hash_murmur3(bufPtr, 4, len, 0)) % mBucketNumber;
		long address = getMemPointer() + ((long)index) * 8;
		CacheScanner.prefetch(address, Prefetch.READ.ordinal(), 
				PrefetchLocality.LOW.ordinal(), 2);
		return index;
	}
	

	/**
	 * Gets the native.
	 *
	 * @param buffer the buffer - contains key
	 * @param bufPtr the buf ptr
	 * @return the native
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected void getNative(ByteBuffer buffer, long bufPtr) throws NativeMemoryException {
		
		buffer.position(0);
		
		boolean found = false;
		
		int len = buffer.getInt();		
		int index = Math.abs(Utils.hash_murmur3(bufPtr, 4, len, 0)) % mBucketNumber;

		// Get Bucket Read Lock
		SpinReadWriteLock lock = getLock(index);
		ReadLock readLock = null;
		if(lock != null){
			readLock = lock.readLock();
			readLock.lock();
		}
		
		try{
			long ptr =IOUtils.getLong(mMemPointer, 8*((long)index));
			
			if(ptr == 0){
				buffer.putInt(0,0);
				return;
			} else{				
				buffer.position(0);
				getIntoBufferPtr(bufPtr, getRealAddress(ptr));
				buffer.position(0);
				if(buffer.getInt() != 0){
					found = true;
					if(isRealEvictionPolicy()){
						// MUST be real address
						long resultPtr = buffer.getLong();
						// Check if it expired
						if(isExpiredEntry(resultPtr)) {
							buffer.position(0);
							buffer.putInt(0);
							//TODO remove entry
							return ;
						}
						// do eviction update
						mEvictionAlgo.hitEntry(resultPtr, lock == null? null:lock.getSpinLock());
					}
				}
				buffer.position(0);
			}
		}finally{
			if(readLock != null) readLock.unlock();
			// update stats
			mTotalCount.incrementAndGet();
			if( found ) mHitCount.incrementAndGet();
		}
	}

	
	
	/**
	 * Gets the lock for key.
	 *
	 * @param key the key
	 * @return the lock for key
	 */
	public SpinReadWriteLock getLockForKey(ByteBuffer key)
	{
		if(isLockFreeMode()) return null;
		key.position(0);
		int length = key.getInt();
		int index = Math.abs(Utils.hash_murmur3(key, 4, length, 0)) % mBucketNumber;
		// Get Bucket Read Lock
		SpinReadWriteLock lock = getLock(index);
		return lock;
	}	
	
	/**
	 * Gets the lock for key.
	 *
	 * @param key the key
	 * @return the lock for key
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public SpinReadWriteLock getLockForKey(Object key) throws NativeMemoryException, IOException
	{
		ByteBufferWithAddress bufa = getLocalBufferWithAddress();
		ByteBuffer buf = bufa.getBuffer();
		buf.clear();
		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		}else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0,  pos -4);
		// TODO - optimize getLockForKey to use optimal hash
		return getLockForKey(buf);
	}	

	
	/**
	 * Execute operation with in-memory data update.
	 *
	 * @param key the key
	 * @param op the op
	 * @return the for update
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	public boolean executeForUpdate(ByteBuffer key, Command<?> op) throws NativeMemoryException, IOException
	{
		SpinReadWriteLock lock = getLockForKey(key);		
		
		WriteLock writeLock = null;
		
		if(lock != null){
			writeLock = lock.writeLock();		
			writeLock.lock();
		}
		
		try{
			return op.execute(key, this);

		}finally{
			if(writeLock != null) writeLock.unlock();
		}
		
	}	
	

	
	/**
	 * Gets the query executor service.
	 *
	 * @return the query executor service
	 */
	static ExecutorService getQueryExecutorService()
	{
		return sQueryExecutor;
	}
	
	/**
	 * Gets the query max processors.
	 *
	 * @return the query max processors
	 */
	static int getQueryMaxProcessors()
	{
		return MAX_QUERY_PROCESSORS;
	}
	

	
	/**
	 * Execute operation without in-memory data update.
	 *
	 * @param key the key
	 * @param op the op
	 * @return the for update
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	public boolean execute(ByteBuffer key, Command<?> op) throws NativeMemoryException, IOException
	{
		SpinReadWriteLock lock = getLockForKey(key);
		ReadLock readLock = null;
		if(lock != null){
			readLock = lock.readLock();
			readLock.lock();
		}
		try{
			return op.execute(key, this);
		}finally{
			if(readLock != null) readLock.unlock();
		}
		
	}
	
	

	/**
	 * Execute.
	 *
	 * @param key the key
	 * @param op the op
	 * @return true, if successful
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean execute(Object key, Command<?> op) throws NativeMemoryException, IOException
	{
		ByteBufferWithAddress bufa = getLocalBufferWithAddress();		
		ByteBuffer buf = bufa.getBuffer();
		buf.clear();
		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		}else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);
		//TODO - optimize hash
		return execute(buf, op);
		
	}
	
	

	/**
	 * Execute for update.
	 *
	 * @param key the key
	 * @param op the op
	 * @return true, if successful
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean executeForUpdate(Object key, Command<?> op) throws NativeMemoryException, IOException
	{
		ByteBufferWithAddress bufa = getLocalBufferWithAddress();		
		ByteBuffer buf = bufa.getBuffer();
		buf.clear();
		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		}else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);
		// TODO - optimize hash
		return executeForUpdate(buf, op);
		
	}
	
	/**
	 * Returns pointer to memory record
	 * 
	 * FORMAT:
	 * 
	 * 0-3 exp time
	 * 4-7 - last access time
	 * 8-15 - next record ptr
	 * 16-19 key length
	 * 20-23 value length
	 * key
	 * value.
	 *
	 * @param key the key
	 * @return the native ptr
	 * @throws NativeMemoryException the native memory exception
	 */
	protected long getNativePtr(ByteBuffer key) throws NativeMemoryException
	{
		return getNativePtr(key, NativeMemory.getBufferAddress(key));
	}
	
	/**
	 * Gets the native ptr.
	 *
	 * @param buffer the buffer
	 * @param bufPtr the buf ptr
	 * @return the native ptr
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected long getNativePtr(ByteBuffer buffer, long bufPtr) throws NativeMemoryException {
		return getNativePtr(buffer, bufPtr, false);
	}
	
	protected long getNativePtr(ByteBuffer buffer, long bufPtr, boolean peek)  {
		buffer.position(0);
		int index = Math.abs(Utils.hash_murmur3(buffer, 4, buffer.getInt(), 0)) % mBucketNumber;

		// Get Bucket Read Lock
		SpinReadWriteLock lock = getLock(index);
		ReadLock readLock = null;
		if(lock != null){
			readLock = lock.readLock();
			readLock.lock();
		}
		try{
			long ptr = IOUtils.getLong(mMemPointer, ((long)index)*8);//mBuffer.get(index);
			
			if(ptr == 0){
				buffer.putInt(0,0);
				return 0;
			} else{				
				buffer.position(0);

				long resultPtr = getPtr(bufPtr, getRealAddress(ptr));
				buffer.position(0);
				if(!peek && resultPtr != 0 && isRealEvictionPolicy()){
					// do eviction update
					mEvictionAlgo.hitEntry(resultPtr, lock == null? null:lock.getSpinLock());
				}
				return resultPtr;
			}
		}finally{
			if(readLock != null) readLock.unlock();

		}
	}
	
	
	/**
	 * Gets the.
	 *
	 * @param buffer the buffer
	 * @throws NativeMemoryException the native memory exception
	 */
	public void get(ByteBuffer buffer) 
	{
		get(buffer, true);
	}
	
	/**
	 * Gets the native.
	 *
	 * @param buffer the buffer
	 * @param touch the touch
	 * @return the native
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected void get(ByteBuffer buffer, boolean touch)  {
		buffer.position(0);
		boolean found = false;
		int keyLen = buffer.getInt(0);
		int index = Math.abs(Utils.hash_murmur3(buffer, 4, keyLen, 0)) % mBucketNumber;
		// Get Bucket Read Lock
		SpinReadWriteLock lock = getLock(index);
		ReadLock readLock = null;
		if(lock != null){
			readLock = lock.readLock();
			readLock.lock();
		}
		try{
			long ptr =  IOUtils.getLong(mMemPointer, ((long)index)*8);//mBuffer.get(index);
			
			if(ptr != 0){
				
				buffer.position(0);
				get(buffer, getRealAddress(ptr));
				// If value not found we change only first 4 bytes?
				buffer.position(0);
				if(buffer.getInt() != 0 ){
					found = true;
					if(isRealEvictionPolicy() && touch){
						//MUST be real address
						long resultPtr = buffer.getLong();
						// Check if it expired
						if(isExpiredEntry(resultPtr)) {
							buffer.position(0);
							buffer.putInt(0);
							//TODO remove entry
							return ;
						}
						// do eviction update
						mEvictionAlgo.hitEntry(resultPtr, lock == null? null:lock.getSpinLock());
					}
				}
			} else{
				buffer.putInt(0,0);
			}
//			boolean notFoundInMemory = buffer.getInt(0)==0;
//			if(notFoundInMemory && checkPersistentStore()){
//				//check persistent store
//				//TODO mark this as "soon be retrived from persistent storage"
//				//TODO we need get value and keep key as well
//				buffer.putInt(0, keyLen);
//			}
//			
//			//buffer.putInt(0,0);
			return;
		
		}finally{
			if(readLock != null) readLock.unlock();
			// Update stats
			mTotalCount.incrementAndGet();
			if(found) mHitCount.incrementAndGet();
		}
	}


	/**
	 * Check persistent store.
	 *
	 * @return true, if successful
	 */
	@SuppressWarnings("unused")
    private boolean checkPersistentStore()
	{
		return mIsPersistent && (
				mPersistenceType == PersistenceMode.WRITE_BEHIND || mPersistenceType == PersistenceMode.WRITE_THROUGH);
	}
	
	
	
	
	/**
	 * Checks if is empty. This method is not blocking,
	 * therefore it is not 100% correct
	 *
	 * @return true, if is empty
	 */
	public boolean isEmpty() {

		return mTotalItems.get() == 0;
	}
	
	
	/**
	 * Size.
	 *
	 * @return the int
	 */
	public long size()
	{
		return mTotalItems.get();
	}
	
	
	/**
	 * Decrement count.
	 */
	public void decrementCount()
	{
		mTotalItems.decrementAndGet();
	}
	
	
	/**
	 * Calculated (approx) total size of a data stored in
	 * in the cache.
	 * @return data size
	 */
	public final long getRawDataSize()
	{
		long size = getAllocatedMemorySize();
		if(mCodec == null) return size;		
		return (long)(size * mCodec.getAvgCompressionRatio());
	}
	
	/**
	 * Gets the compressed data size.
	 *
	 * @return the compressed data size
	 */
	public final long getCompressedDataSize()
	{
		return getAllocatedMemorySize();
	}
	
	/**
	 * Gets the average compression ratio.
	 *
	 * @return the average compression ratio
	 */
	public final double getAverageCompressionRatio()
	{
		if(mCodec == null && (storageOptimizerList == null || storageOptimizerList.size() == 0)) return 1.;
		if(storageOptimizerList == null || storageOptimizerList.size() == 0){
		  return mCodec.getAvgCompressionRatio();
		}
		double baseCompRatio = ( mCodec == null)? 1.0: mCodec.getAvgCompressionRatio();
		double recompressedRatio = storageOptimizerList.get(0).getEstimatedRecompressedRatio();
		double optimizerCompRatio = storageOptimizerList.get(0).getEstimatedCompressionRatio();
		return (1- recompressedRatio) * baseCompRatio + recompressedRatio * optimizerCompRatio;
		
	}
	
	/**
	 * Gets the memory size.
	 *
	 * @return the memory size
	 */
	public final long getAllocatedMemorySize()
	{
		return mMemorySize.get();
	}
	
	/**
	 * Gets the total memory size.
	 *
	 * @return the total memory size
	 */
	public final long getTotalAllocatedMemorySize()
	{
		return mBucketNumber*8 + getAllocatedMemorySize();
	}
	
	/**
	 * Update allocated memory.
	 *
	 * @param delta the delta
	 */
	public final void updateAllocatedMemory(long delta)
	{
		mMemorySize.addAndGet(delta);
		if(sGlobalMaxMemory > 0){
		  sGlobalMemorySize.addAndGet(delta);
		}
	}
	/**
	 * Test only
	 */
	    
	public static void resetGlobals(){
	  sGlobalMemorySize.set(0);
	}
	
	/**
	 * Check out of memory.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 */
	private void checkOutOfMemory() throws NativeMemoryException{
		long allocated = mMemorySize.get();
		if(mEvictionPolicy == EvictionPolicy.NONE && !cacheIsInfinite()){
			if(allocated > mMaxMemory){
				throw new NativeMemoryException("Out Of Memory");
			}
		} 
	}
	
	
	public static boolean isGlobalEvictionActive()
	{
	  return sEvictionActive.get();
	}
	
	public static void checkGlobalEviction()
	{
	  
	  if(sGlobalMaxMemory == 0){
	    return;
	  }
	  boolean active = sEvictionActive.get();
	  long globalUsed = sGlobalMemorySize.get();
	  if( globalUsed < sLowWatermark * sGlobalMaxMemory){
	    sEvictionActive.set(false);
	  } else if( active == false && globalUsed > sHighWatermark * sGlobalMaxMemory) {
	    sEvictionActive.set(true);
	  }	  
	}
	
	/**
	 * Checks if is eviction active.
	 *
	 * @return true, if is eviction active
	 */
	public boolean isEvictionActive()
	{
		return mEvictionActive.get();
	}
	
	/**
	 * Checks if is eviction auto.
	 *
	 * @return true, if is eviction auto
	 */
	public boolean isEvictionAuto()
	{
		return mEvictionAuto.get();		
	}
	
	/**
	 * Sets the eviction auto.
	 * If evictionAuto = false and eviction is enabled,
	 * local cache limit is ignored and cache can grow until 
	 * global allocation reaches global max limit.
	 *   
	 * @param v the new eviction auto
	 */
	public void setEvictionAuto(boolean v)
	{
		mEvictionAuto.set(v);
	}
	
	
	/**
	 * Update eviction status.
	 */
	private void updateEvictionStatus()
	{
		// Check global Malloc limit first
    // FIXME: expensive code
	    Malloc malloc = NativeMemory.getMalloc();
	    long maxMemory = malloc.getMaxMemorySize();
	    long globalAllocd = malloc.memoryAllocated();
	    if(globalAllocd > mHighWatermark * maxMemory){
	      mEvictionActive.set(true);
	      return;
	    }
		
	  
	  long allocated = getTotalAllocatedMemorySize();
		boolean evictionStatus = false;
		if(allocated > mHighWatermark*mMaxMemory && (mMaxMemory > 0) && isEvictionAuto()){
			evictionStatus= true;
		} else if(allocated < mLowWatermark*mMaxMemory || (mMaxMemory == 0) || isEvictionAuto() == false){
			evictionStatus = false;
		}		
		if(( !evictionStatus) && sGlobalMaxMemory > 0){
			// Check global
			long globalAllocated = sGlobalMemorySize.get();
			if(globalAllocated > mHighWatermark*sGlobalMaxMemory){
				evictionStatus= true;
			} else if(globalAllocated < mLowWatermark*sGlobalMaxMemory){
				evictionStatus = false;
			}		
		}

		mEvictionActive.set(evictionStatus);
	}
	
	/**
	 * Process eviction.
	 *
	 * @param index the index
	 * @return address of evicted item, -1 - if eviction is N/A; 0 - if eviction failed for some
	 * reason (no candidate was found)
	 * @throws NativeMemoryException the j emalloc exception
	 */
	Random r = new Random();
  boolean runFastEviction = false;
  
  private long processEviction(int index) throws NativeMemoryException {
    
    if( runFastEviction ||  mEvictionAlgo instanceof EvictionLRU2Q == false || mCacheConfig.getLRU2QInsertPoint() >= 0.5){
      // Short circuit for Non-LRU2Q and LRU2Q with high insert point 
      runFastEviction = true;
      return processEvictionFast(index);
    }
    
    // Eviction stuff starts
    checkOutOfMemory();

    Unsafe unsafe = NativeMemory.getUnsafe();

    if (cacheIsInfinite())
      return -1;
    if (!isRealEvictionPolicy())
      return -1;
    updateEvictionStatus();
    int lockIndex = getLockIndex(index);
    long evictedPtr = 0;

    if (mEvictionActive.get()) {

      int currentAttempt = 0;
      final int nr = r.nextInt(mBucketWidth);
      int idx = lockIndex * mBucketWidth + nr;// index,
      final int totalAttempts = mBucketWidth;
      final int inc = 1;
      long mem = 0;

      int currentCandidateIndex = -1;
      long currentEvData = -1;
      long currentExpire = -1;
      long currentPtr = 0;
      long prevPtr    = 0;
      int totalScanned = 0;
      long realBufferAddress = NativeMemory.lockAddress(mMemPointer);

      try {

        //LOG.info("ProcessEviction starts:"+index);
        while (currentAttempt++ < totalAttempts) {
          // mem = IOUtils.getLong(mMemPointer, ((long)idx) * 8);
          mem = unsafe.getLong(realBufferAddress + ((long) idx) * 8);
          long curEvData = -1;
          long curExpire = -1;
          
          if (mem != 0) {
            //checkDebug(realBufferAddress + ((long) idx) * 8);
            long ptr = mem;
            currentPtr = ptr;
            prevPtr = 0;
            while (ptr != 0L) {

              long realPtr = NativeMemory.lockAddress(ptr);
              // long expire = IOUtils.getUInt(ptr, 0);
              long expire = org.yamm.util.Utils.getInt(realPtr);
              // Do not put pinned objects into eviction candidate list
              if (expire == IMMORTAL) {
                NativeMemory.unlockAddress(ptr);
                // DO NOTHING for MallocNoCompaction
                // TODO FIX IT! TEST IT!
                //continue;
                prevPtr = ptr;
                //currentPtr = ptr;
                ptr = getNextAddressRaw(ptr);
                continue;
              }
              totalScanned++;
              // long evData = IOUtils.getUInt(ptr, 4);
              long evData = org.yamm.util.Utils.getUInt(realPtr + 4);
              int result = mEvictionAlgo.selectBetterCandidate(curEvData,
                  curExpire, evData, expire);
              if (result == 2) {
                //currentCandidateIndex = idx;
                //LOG.info("CCI="+currentCandidateIndex);
                curEvData = evData;
                curExpire = expire;  
 
                if(debug){
                  LOG.info("PRE-SWAP");
                  debug = true;
                  dumpList(realBufferAddress + ((long) idx) * 8);
                }
                swapPointers(realBufferAddress + ((long) idx) * 8, currentPtr, prevPtr, ptr);
                if(debug){
                  LOG.info("POST-SWAP");
                  dumpList(realBufferAddress + ((long) idx) * 8);
                  debug = false;
                }
                long tmp = currentPtr;
                currentPtr = ptr;
                if(tmp != 0L) ptr = tmp;
              }
              prevPtr = ptr;
              NativeMemory.unlockAddress(ptr);
              // Do nothing for malloc no compaction
              //currentPtr = ptr;
              ptr = getNextAddressRaw(ptr);
            }
            
          }
          
          int result = mEvictionAlgo.selectBetterCandidate(currentEvData,
              currentExpire, curEvData, curEvData); 
          
          if( result == 2 && curEvData >=0){
            currentCandidateIndex = idx;
            currentEvData = curEvData;
            currentExpire = curExpire;
          }
          
          debug = false;
          //LOG.info("List:");
          //dumpList(realBufferAddress + ((long) idx) * 8);
          //LOG.info("Algo:");
          verifyAlgo(realBufferAddress + ((long) idx) * 8);
          
          if (totalScanned >= mEvCandidateListSize 
              || currentAttempt == totalAttempts && currentCandidateIndex >= 0/* Small cache has bucketWidth < mEvCandidateListSize*/) {
            // do eviction
            //LOG.info("Evict starts:"+currentCandidateIndex);           
            evictedPtr = mEvictionAlgo.evict(currentCandidateIndex);
            //LOG.info("Evict finished:"+index);
            break;
          }
          
          idx += inc;
          if (idx == (lockIndex + 1) * mBucketWidth)
            idx = lockIndex * mBucketWidth;
        }

      } finally {
        NativeMemory.unlockAddress(mMemPointer);
        // Do nothing for malloc no compaction
      }
     //LOG.info("ProcessEvicts finished:"+index);

      return evictedPtr;
    }

    return -1;

  }

  private long processEvictionFast(int index) throws NativeMemoryException {
    // Eviction stuff starts
    checkOutOfMemory();

    Unsafe unsafe = NativeMemory.getUnsafe();

    if (cacheIsInfinite())
      return -1;
    if (!isRealEvictionPolicy())
      return -1;
    updateEvictionStatus();
    int lockIndex = getLockIndex(index);
    long evictedPtr = 0;

    if (mEvictionActive.get()) {

      int currentAttempt = 0;
      final int nr = r.nextInt(mBucketWidth);
      int idx = lockIndex * mBucketWidth + nr;// index,
      final int totalAttempts = mBucketWidth;
      final int inc = 1;
      long mem = 0;

      int currentCandidateIndex = -1;
      long currentEvData = -1;
      long currentExpire = -1;
      int totalScanned = 0;
      long realBufferAddress = NativeMemory.lockAddress(mMemPointer);

      try {

        while (currentAttempt++ < totalAttempts) {
          mem = unsafe.getLong(realBufferAddress + ((long) idx) * 8);

          if (mem != 0) {
            long ptr = mem;

            long realPtr = NativeMemory.lockAddress(getRealAddress(ptr));
            long expire = org.yamm.util.Utils.getInt(realPtr);
            // Do not put pinned objects into eviction candidate list
            if (expire == IMMORTAL) {
              // skip it
              NativeMemory.unlockAddress(ptr);
              // DO NOTHING for MallocNoCompaction
              // TODO FIX IT! TEST IT!
              idx += inc;
              if (idx == (lockIndex + 1) * mBucketWidth){
                idx = lockIndex * mBucketWidth;
              }
              continue;
            }
            totalScanned++;
            // long evData = IOUtils.getUInt(ptr, 4);
            long evData = org.yamm.util.Utils.getUInt(realPtr + 4);
            int result = mEvictionAlgo.selectBetterCandidate(currentEvData,
                currentExpire, evData, expire);
            if (result == 2) {
              currentCandidateIndex = idx;
              // LOG.info("CCI="+currentCandidateIndex);
              currentEvData = evData;
              currentExpire = expire;

            }
            NativeMemory.unlockAddress(ptr);
            // Do nothing for malloc no compaction
          }

          if (totalScanned >= mEvCandidateListSize 
              || currentAttempt == totalAttempts  && currentCandidateIndex >= 0/* Small cache has bucketWidth < mEvCandidateListSize*/) {
            // do eviction
            evictedPtr = mEvictionAlgo.evict(currentCandidateIndex);
            break;
          }

          idx += inc;
          if (idx == (lockIndex + 1) * mBucketWidth){
            idx = lockIndex * mBucketWidth;
          }
        }

      } finally {
        NativeMemory.unlockAddress(mMemPointer);
        // Do nothing for malloc no compaction
      }

      return evictedPtr;
    }

    return -1;

  }
  
static long counter = 0;
static long abnormalZero = 0;
static long abnormalNonZero = 0;
static long firstZero = 0;


 public static void dumpStats()
 {
   LOG.info("C: "+counter+" AZ: "+abnormalZero+" FZ: "+firstZero+" ANZ: "+abnormalNonZero);
 }
 
 public static void resetStats()
 {
   counter=0;
   abnormalZero=0;
   abnormalNonZero=0;
   firstZero =0;
 }
	private void verifyAlgo(long start) {

	  counter++;
    long ptr = getRealAddress(org.yamm.util.Utils.getLong(start));
    if(ptr == 0){
      //LOG.info("V-D: "+start);
      return;
    }
    @SuppressWarnings("unused")
    boolean exit = false; 
    @SuppressWarnings("unused")
    boolean found = false;
    long firstEvData = IOUtils.getUInt(getRealAddress(ptr), 4);
    if(firstEvData == 0) firstZero++;
    //LOG.info("start: "+firstEvData);
    while(ptr != 0){
      //LOG.info("ptr="+NativeMemory.lockAddress(ptr));
      long data = IOUtils.getUInt(ptr, 4);
      //LOG.info("data: "+data);
      if(data < firstEvData){
        //LOG.info("Algo failed"); exit = true;
        if(data == 0){
          abnormalZero++;
        } else{
          abnormalNonZero++;
          found = true;
          //LOG.info("ANZ="+data);;
        }        
      }
      ptr = getNextAddress(ptr);

    }
    
  }

	// Total >= 4 Zeroes >=2
	@SuppressWarnings("unused")
  private void checkDebug(long start){
    long ptr = org.yamm.util.Utils.getLong(start);
    if(ptr == 0) return;
    int total =0, zeros = 0;
    while(ptr != 0){
      total++;
      long data = IOUtils.getUInt(ptr, 4); 
      if(data ==  0) zeros++;
      ptr = getNextAddress(ptr);

    }
    debug = total>=4 && zeros >=2;
	}
	
  private void dumpList(long start) {     
	  long ptr = org.yamm.util.Utils.getLong(start);
	  long prevPtr = 0;
	  boolean exit = false;
	  while(ptr != 0){
	    long realPtr = getRealAddress(ptr);
	    LOG.info(ptr+" :"+NativeMemory.lockAddress(realPtr)+" data="+IOUtils.getUInt(realPtr, 4));
	    NativeMemory.unlockAddress(realPtr);
	    prevPtr = ptr;
	    ptr = getNextAddressRaw(ptr);
	    if(ptr == prevPtr) {
	      LOG.info(" loop detected");
	      System.exit(-1);
	    }
	    if( ptr != 0 && prevPtr > 0){
	      // PrevPtr MUST be < 0
	      LOG.info("Wrong sign detected");
	      //System.exit(-1);
	      exit = true;
	    }
	  }
	  if(exit) System.exit(-1);
  }

  private boolean debug = false;
  
  private void swapPointers(long base, long currentPtr, long prevPtr, long ptr) {
    // TODO Auto-generated method stub
	  if(debug){
    LOG.info("swap "+base+" currentPtr "+NativeMemory.lockAddress(getRealAddress(currentPtr))+
	      " prevPtr "+NativeMemory.lockAddress(getRealAddress(prevPtr))+" ptr "+NativeMemory.lockAddress(getRealAddress(ptr)));
	  }
    if(currentPtr == ptr ) return;
	  // Both pointers are raw
	  // 1. put ptr into base (mark high bit)
	  org.yamm.util.Utils.putLong(base, ptr | 0x8000000000000000L);
	  long nextRaw = getNextAddressRaw(getRealAddress(ptr));
	  long currentNextRaw = getNextAddressRaw(getRealAddress(currentPtr));
	  if(debug){
	    LOG.info("SWAP : "+ NativeMemory.lockAddress(getRealAddress(org.yamm.util.Utils.getLong(base)))) ;	  
	    LOG.info("RAW: nextRaw "+nextRaw+ " curNextRaw " + currentNextRaw+" currentPtr "+currentPtr);
	    LOG.info(" curNext "+ NativeMemory.lockAddress(getRealAddress(currentNextRaw)));
	    LOG.info(" next "+ NativeMemory.lockAddress(getRealAddress(nextRaw)));
	  }
	  // 2. put currentNextRaw into ptr
	  if(prevPtr != currentPtr){
	    IOUtils.putLong(getRealAddress(ptr), NP_OFFSET, currentNextRaw);
	    IOUtils.putLong(getRealAddress(currentPtr), NP_OFFSET, nextRaw);
	    IOUtils.putLong(getRealAddress(prevPtr), NP_OFFSET, (nextRaw != 0)? currentPtr : getRealAddress(currentPtr) );
	  } else{
	     IOUtils.putLong(getRealAddress(ptr), NP_OFFSET, (nextRaw != 0)? currentPtr : getRealAddress(currentPtr));
	     IOUtils.putLong(getRealAddress(currentPtr), NP_OFFSET, nextRaw);
	  }    
  }

  /**
	 * Cache is infinite.
	 *
	 * @return true, if successful
	 */
	private boolean cacheIsInfinite() {
		
		return mMaxMemory == 0 && sGlobalMaxMemory == 0;
	}

	/**
	 * Gets the next address.
	 *
	 * @param ptr the ptr
	 * @return the next address
	 */
	protected final long getNextAddress(final long ptr)
	{
		long addr = getRealAddress(ptr);
		long nextRaw = IOUtils.getLong(addr, NP_OFFSET);
		long next = nextRaw & 0x7fffffffffffffffL;
		return  next;
	}
	
	/**
	 * Gets the next address raw.
	 *
	 * @param ptr the ptr
	 * @return the next address raw
	 */
	public final long getNextAddressRaw(final long ptr)
	{
		return IOUtils.getLong(getRealAddress(ptr), NP_OFFSET);
	}	
	

			
	/**
	 * Sets the next ptr.
	 *
	 * @param ptr the ptr
	 * @param nextPtr the next ptr
	 */
	private final void setNextPtr(final long ptr, long nextPtr) {

		setNextPtr(ptr, nextPtr, false);
		
	}

	/**
	 * Sets the next ptr.
	 *
	 * @param ptr the ptr
	 * @param nextPtr the next ptr
	 * @param set the set
	 */
	private final void setNextPtr(final long ptr, long nextPtr, boolean set) {

		long realPtr = getRealAddress(ptr);
		if(realPtr == getRealAddress(nextPtr)){
			LOG.info("fuck");
		}
		if(set) nextPtr = nextPtr | 0x8000000000000000L;
		IOUtils.putLong(realPtr, NP_OFFSET, nextPtr);

		
	}	
	
	private final long getLastAddressInChain(final long ptr)
	{
	  long address = 0;
	  long last = getRealAddress(ptr);
	  while ((address = IOUtils.getLong(last, NP_OFFSET)) != 0){
	    last = getRealAddress(address);
	  }
	  return last;
	}

	
	/**
	 * Check set bit.
	 *
	 * @param val1 the val1
	 * @param val2 the val2
	 * @return the long
	 */
	private final long checkSetBit(long val1,  long val2)
	{
		if((val1 & 0x8000000000000000L) != 0)
		{
			val2 |= 0x8000000000000000L;
		}
		return val2;
	}
	

	
	/**
	 * Format:
	 * 0-3 keyLength
	 * 4-7 valLength
	 * key
	 * value.
	 *
	 * @param keyValue the key value
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void put(ByteBuffer keyValue) throws NativeMemoryException
	{
		put(keyValue, mExpireDefault);
	}
	
	
	/**
	 * Put with expire.
	 *
	 * @param kv the kv
	 * @return true, if successful
	 * @throws NativeMemoryException the native memory exception
	 */
	public boolean putWithExpire(ByteBuffer kv) throws NativeMemoryException
	{
		int kl= kv.getInt(0);
		int vl = kv.getInt(4);
		long expTime = kv.getLong(8+kl+(vl -8));
		if(expTime > System.currentTimeMillis() ){
			return false;
		}
		kv.putInt(4, vl-8);
		put(kv, expTime);
		return true;
	}
	
	
	private AtomicLong failedEvictionAttemptCount = new AtomicLong(0);
	private AtomicLong totalEvictionAttemptCount = new AtomicLong(0);
	private AtomicLong failedFatalEvictionAttemptCount = new AtomicLong(0);
	
	
	public long getTotalEvictionAttempts()
	{
		return totalEvictionAttemptCount.get();
	}
	
	public long getFailedEvictionAttempts()
	{
		return failedEvictionAttemptCount.get();
	}
	
	public long getFailedFatalEvictionAttempts()
	{
		return failedFatalEvictionAttemptCount.get();
	}
	
	
	public void getDecompress(ByteBuffer buf) throws NativeMemoryException, IOException
	{

		if(mCompressionEnabled == false || mCodec == null){
			get(buf);
		} else{
			ByteBuffer temp = getLocalBufferWithAddress().getBuffer();
			temp.clear();
			int keySize = buf.getInt(0);
			buf.position(4);
			buf.limit(4 + keySize);
			temp.putInt(keySize);
			temp.put(buf);
			get(temp);

			if(temp.getInt(0) == 0){
				// not found
				buf.putInt(0, 0);
				return;
			}
			// do decompression
			// 1) copy first 12 bytes
			buf.clear();
			
			temp.position(0);
			temp.limit(12);
			buf.put(temp);
			temp.limit(temp.capacity());
			// Read compSize;
			int compSize = temp.getInt(12);
			int valSize = temp.getInt(0);

			temp.position(16);
			temp.limit(16 + valSize - 4);
			
			if(compSize == 0){
				// No compression				

				buf.putInt(0, valSize - 4);
				buf.put(temp);
			} else{
				buf.position(12);
				mCodec.decompress(temp, buf);
				buf.putInt(0, buf.limit() - 12);
			}
		}
	}
	
	public void putCompress(ByteBuffer keyValue, long expire) throws NativeMemoryException, IOException
	{
		if(mCompressionEnabled == false || mCodec == null ){
			put(keyValue, expire);
		} else{
			ByteBuffer buf = getLocalBufferWithAddress().getBuffer();
			buf.clear();
			int valSize = keyValue.getInt(4);
			int minSizeToCompress = mCodec.getCompressionThreshold();
			int keySize = keyValue.getInt(0);
			// Copy key
			buf.putInt(0, keySize);
			buf.position(8);
			keyValue.position(8);
			keyValue.limit(8 + keySize);
			buf.put(keyValue);
			keyValue.position(8 + keySize);
			keyValue.limit(8 + keySize + valSize);
			// buf is already positioned
			int compSize = 0;
			if(valSize < minSizeToCompress){
				// no compression
				buf.putInt(0);
				//buf.put((byte) 0);
				buf.put(keyValue);
				compSize = 4 + valSize;
			} else{
				int pos = buf.position();
				buf.position(pos + 4);
				compSize = mCodec.compress(keyValue, buf);
				buf.putInt(pos, compSize);
				compSize += 4;
			}
			// Put comp size
			buf.putInt(4, compSize);			
			
			put(buf, expire);
		}
	}
	

	
	/**
	 * Format:
	 * 0-3 keyLength
	 * 4-7 valLength
	 * key
	 * value.
	 *
	 * @param keyValue the key value
	 * @param expire the expire
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public void put(ByteBuffer keyValue, long expire) throws NativeMemoryException
	{
		//resetPutTrialCounter();

		long putPtr =0L;
		keyValue.position(0);
		int keyLen = keyValue.getInt();

		int index = Math.abs(Utils.hash_murmur3(keyValue, 8, keyLen, 0)) % mBucketNumber;
		// Get Bucket Read Lock
		SpinReadWriteLock lock = getLock(index);
		WriteLock writeLock = null;
		
		if(lock != null){
			writeLock = lock.writeLock();
			writeLock.lock();

		}

		long evictedPtr = 0;
		evictedPtr = processEviction(index);

		// Notify eviction listener
		if( evictedPtr > 0 && mEvictionListener != null){
			mEvictionListener.evicted(evictedPtr, Reason.ALGO, 0L /*nano time?*/);
		}
		
		if( mEvictionActive.get() == true){
			totalEvictionAttemptCount.incrementAndGet();
			if(evictedPtr == 0){
				failedEvictionAttemptCount.incrementAndGet();
			} else if( evictedPtr == -1){
				failedFatalEvictionAttemptCount.incrementAndGet();
			}
		}
		
		long evictedSize = (evictedPtr > 0)? NativeMemory.mallocUsableSize(evictedPtr): 0;
		
		boolean canTryRecycle =  (evictedPtr > 0);

		try{
			long ptr = IOUtils.getLong(mMemPointer, ((long)index)*8);
			long realPtr = getRealAddress(ptr);
			
			if(ptr == 0) {
				// Yes, its possible: current bucket [index] is 0, 
			  // we insert new object as a head of a list

				if(!canTryRecycle){
					putPtr = copyToOffHeap(keyValue);
					// we do not have to delete evicted as its 0.

				} else{
				  // it frees evictedPtr
					putPtr = copyToOffHeapRecycle(keyValue, evictedPtr);	
				}
				// Check if ptr == 0L and return.
				// We had a failed put due to memory allocation failure
				if(putPtr == 0L) {
				  // If failed to insert NEW its OK, but if we failed to update existing key?
				  // We need to delete this key
				  //mPutFailed.incrementAndGet(); 
				  return;
				}
				
				IOUtils.putLong(mMemPointer, ((long)index)*8, putPtr);
				// Increment total count
				mTotalItems.incrementAndGet();
				updateAllocatedMemory(NativeMemory.mallocUsableSize(putPtr)-evictedSize);
				// Do eviction init
				if(isRealEvictionPolicy()){
					mEvictionAlgo.initEntry(putPtr, expire, lock == null? null:lock.getSpinLock());
				}

				
			} else{

				// scan the list; find the key (realloc) or append to 
				// OK, ptr != null get the key and compare with key
				int keySize = IOUtils.getInt(realPtr, 0 + OFFSET);

				final boolean equalLengths = (keyLen == keySize);
				if(equalLengths && Utils.equals(keyValue, 8, keyLen, 
						NativeMemory.memread2ea(realPtr, 8 + OFFSET, keySize, getLocalKeyStore(keySize))))
				{ // PUT is update of 'ptr'
					// This is a rare situation anyway
					// update operation
					if(!isRealEvictionPolicy()){	// Means NO EVICTION/ NO FREE
						// TODO nextPtr
						long oldSize = NativeMemory.mallocUsableSize(realPtr);
						long nextPtr = IOUtils.getLong(realPtr, NP_OFFSET);
						// copy to off heap with realloc preserves 1st bit of ptr
						putPtr = copyToOffHeap(keyValue, ptr);
						
		        // Check if ptr == 0L and return.
		        // We had a failed put due to memory allocation failure
		        if(putPtr == 0L) {
		          //mPutFailed.incrementAndGet();
		          return;
		        }
		        
						long newSize = NativeMemory.mallocUsableSize(getRealAddress(putPtr));
						updateAllocatedMemory(newSize - oldSize);
						if( nextPtr != 0){
							// TODO PUT nextPtr as head of a list
	            long lastInChain = getLastAddressInChain(nextPtr);
							IOUtils.putLong(lastInChain, NP_OFFSET, putPtr);
	            IOUtils.putLong(mMemPointer, ((long)index) * 8, nextPtr | 0x8000000000000000L);

						} else{
							IOUtils.putLong(mMemPointer, ((long)index) * 8, putPtr);
						}
						
					} else{					
						long oldSize = NativeMemory.mallocUsableSize(realPtr);
						long evData = IOUtils.getLong(realPtr, 0);
						// copy to off heap with realloc preserves 1st bit
						long nextPtr = IOUtils.getLong(realPtr, NP_OFFSET);
						
						putPtr = copyToOffHeap(keyValue, ptr);
		        // Check if ptr == 0L and return.
		        // We had a failed put due to memory allocation failure
		        if(putPtr == 0L) {
		          //mPutFailed.incrementAndGet(); 
		          return;
		        }
						long realPutPtr = getRealAddress(putPtr);
						
						IOUtils.putLong(realPutPtr, 0, evData);

						if( nextPtr != 0){
              // TODO PUT nextPtr as head of a list

	            long lastInChain = getLastAddressInChain(nextPtr);
	            IOUtils.putLong(lastInChain, NP_OFFSET, putPtr);
	            IOUtils.putLong(mMemPointer, ((long)index) * 8, nextPtr | 0x8000000000000000L);
						} else{
							IOUtils.putLong(mMemPointer, ((long)index) * 8, putPtr);
						}
						long newSize = NativeMemory.mallocUsableSize(realPutPtr);
						updateAllocatedMemory(newSize - oldSize);
						
						// Do eviction init
						mEvictionAlgo.hitEntry(getRealAddress(putPtr), lock == null? null:lock.getSpinLock());
					}
					// We need to free evicted item
					// TODO only if evictedPtr != ptr???
					// ITS ISSUE
					if(canTryRecycle){
						NativeMemory.free(evictedPtr);
						updateAllocatedMemory(-evictedSize);
					}
					
				} else{ // PUT is not update of 'ptr'
					long nextPtr = getNextAddressRaw(ptr);
					if(nextPtr != 0){
						// both addresses are raw: contains first bits
						putPtr = findAndPut(ptr, nextPtr, keyValue, lock == null? null:lock.getSpinLock(), expire, evictedPtr);
		        // Check if ptr == 0L and return.
		        // We had a failed put due to memory allocation failure
		        if(putPtr == 0L) {
		          //mPutFailed.incrementAndGet(); 
		          return;
		        }
					} else{
						if(!canTryRecycle){
							putPtr = copyToOffHeap(keyValue);
						} else{
						  // It frees evictedPtr
							putPtr = copyToOffHeapRecycle(keyValue, evictedPtr);	
						}
		        // Check if ptr == 0L and return.
		        // We had a failed put due to memory allocation failure
		        if(putPtr == 0L) {
		          //mPutFailed.incrementAndGet(); 
		          return;
		        }
						// set next ptr
						
						IOUtils.putLong(mMemPointer, ((long)index) * 8, ptr | 0x8000000000000000L);
						setNextPtr(ptr, putPtr);
						// Increment total count

						mTotalItems.incrementAndGet();
						updateAllocatedMemory(NativeMemory.mallocUsableSize(getRealAddress(putPtr))-evictedSize);
						// Do eviction init
						if(isRealEvictionPolicy()){
							mEvictionAlgo.initEntry(getRealAddress(putPtr), expire, lock == null? null:lock.getSpinLock());
						}
					}
				}
			}			
		} finally{
			
		  boolean putFailed = putPtr == 0L;
			
			if(writeLock != null) writeLock.unlock();
			
			if(putFailed) {
//			  int currentAttempt = putAttemptCounter.get();
//
//			  // evict one more object
//			  // while size is > 0
//			  // and try to put again
//			  if(size() > 0 && currentAttempt < MAX_PUT_ATTEMPTS) {
//			    evictObject();
//			    // recursive invocation
//			    putAttemptCounter.set(++currentAttempt);
//			    put(keyValue, expire);
//			  } else{
			    removeWhenPutFailed(keyValue);
			  //}
			}
			//resetPutTrialCounter();
		}
	}
	
	@SuppressWarnings("unused")
  private final void resetPutTrialCounter() {
	  putAttemptCounter.set(0);    
  }
	
  static ThreadLocal<Integer> putAttemptCounter = new ThreadLocal<Integer>(){

    /* (non-Javadoc)
     * @see java.lang.ThreadLocal#initialValue()
     */
    @Override
    protected Integer initialValue() {
      return new Integer(0);
    }
	  
	};
	
	@SuppressWarnings("unused")
  private void updateEvictionThresholds(long memoryAllocated) {
	  // TODO Auto-generated method stub
	  
  
	}

  private void removeWhenPutFailed(ByteBuffer keyValue)  {
	  int keySize = keyValue.getInt(0);
	  
	  mPutFailed.incrementAndGet();
	  
	  // Move key data by -4 from keyValue
	  for(int i= 8 ; i < 8 + keySize; i++)
	  {
	     keyValue.put( i -4, keyValue.get(i));
	  }
	  
    try{
	    remove(keyValue);
	  } catch(Exception e){
	    throw new RuntimeException("failed to remove object on update failure.");
	  }
	}

  /**
	 * Gets the local key store.
	 *
	 * @param reqSize the req size
	 * @return the local key store
	 */
	private final byte[] getLocalKeyStore(int reqSize) {
		byte[] buf = OffHeapCache.sKeyStoreTLS.get();
		if(buf.length >= reqSize) return buf;
		buf = new byte[reqSize];
		OffHeapCache.sKeyStoreTLS.set(buf);
		return buf;
	}

	/**
	 * Find and put.
	 *
	 * @param prevPtr the prev ptr
	 * @param ptr the ptr
	 * @param keyValue the key value
	 * @param lock the lock
	 * @param expire the expire
	 * @param evictPtr the evict ptr
	 * @throws NativeMemoryException the j emalloc exception
	 */
	private long findAndPut(long prevPtr, long ptr, ByteBuffer keyValue,
			SpinLock lock, long expire, long evictPtr) throws NativeMemoryException
	{		

		int keySize = keyValue.getInt(0);
		keyValue.position(0);
		long putPtr = 0L;
		long evictedSize = (evictPtr > 0) ? NativeMemory.mallocUsableSize(evictPtr): 0;				
		boolean canTryReuse = (evictPtr > 0);

		long realPtr = getRealAddress(ptr);
		int otherKeySize = IOUtils.getInt(realPtr, 0 + OFFSET);
		
		final boolean equalSize = (keySize == otherKeySize); 
		if (equalSize && Utils.equals(keyValue, 8, keySize, NativeMemory.memread2ea(realPtr, 
					8 + OFFSET, otherKeySize, getLocalKeyStore(otherKeySize)))) {
			// reallocate - update op
			if (!isRealEvictionPolicy()) {
				long oldSize = NativeMemory.mallocUsableSize(realPtr);
				long nextPtr = IOUtils.getLong(realPtr, NP_OFFSET);
				putPtr = copyToOffHeap(keyValue, ptr);
				
				if(putPtr == 0L){
				  //mPutFailed.incrementAndGet(); 
				  return 0L;
				}
				
				long newSize = NativeMemory.mallocUsableSize(getRealAddress(putPtr));
				updateAllocatedMemory(newSize - oldSize);				
				if(nextPtr != 0L){
				  //TODO WE HAVE TO INSERT putPtr after nextPtr
					//setNextPtr(prevPtr, putPtr, true);
					//setNextPtr(putPtr, nextPtr);
          setNextPtr(prevPtr, nextPtr, true);
          long lastInChain = getLastAddressInChain(nextPtr);
          setNextPtr( lastInChain, putPtr);

				} else{
					setNextPtr(prevPtr, putPtr);
				}
			} else {
				long oldSize = NativeMemory.mallocUsableSize(realPtr);
				long evData = IOUtils.getLong(realPtr, 0);
				long nextPtr = IOUtils.getLong(realPtr, NP_OFFSET);
				
				putPtr = copyToOffHeap(keyValue, ptr);
        if(putPtr == 0L){
          mPutFailed.incrementAndGet(); return 0L;
        }
				long realPutPtr = getRealAddress(putPtr);
				IOUtils.putLong(realPutPtr, 0, evData);
				long newSize = NativeMemory.mallocUsableSize(realPutPtr);
				updateAllocatedMemory(newSize - oldSize);

				if(nextPtr != 0L){
          //TODO WE HAVE TO INSERT putPtr after nextPtr

					//setNextPtr(prevPtr, putPtr, true);
					//setNextPtr(putPtr, nextPtr);
          setNextPtr(prevPtr, nextPtr, true);
          //setNextPtr( nextPtr, putPtr);
          long lastInChain = getLastAddressInChain(nextPtr);
          setNextPtr( lastInChain, putPtr);
				} else{
					setNextPtr(prevPtr, putPtr);
				}
				// Do eviction init
				mEvictionAlgo.hitEntry(realPutPtr, lock);
			}
			if (canTryReuse) {
				NativeMemory.free(evictPtr);
				updateAllocatedMemory(-evictedSize);
			}
		} else {
			// recursion
			long nextPtr = getNextAddressRaw(ptr);
			if (ptr > 0 && nextPtr != 0) {
				//TODO why is it superfuck?
			  //LOG.error("superfuck");
			}
			if (nextPtr == 0) {
				setNextPtr(prevPtr, ptr, true);
				if (!canTryReuse) {
					putPtr = copyToOffHeap(keyValue);
				} else {
					putPtr = copyToOffHeapRecycle(keyValue, evictPtr);
				}
        if(putPtr == 0L){
          mPutFailed.incrementAndGet(); return 0L;
        }
				// New
				setNextPtr(ptr, putPtr);
				// Increment total count
				mTotalItems.incrementAndGet();
				// increment total cache size
				updateAllocatedMemory(NativeMemory.mallocUsableSize(putPtr)
						- evictedSize);
				// Do eviction init
				if (isRealEvictionPolicy()) {
					mEvictionAlgo.initEntry(putPtr, expire, lock);
				}
			} else {
				setNextPtr(prevPtr, ptr, true);
				putPtr = findAndPut(ptr, nextPtr, keyValue, lock, expire, evictPtr);
			}
		}
		return putPtr;
		
	}

	/**
	 * Copy to off heap.
	 *
	 * @param keyValue the key value
	 * @param reallocPtr the realloc ptr
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected long copyToOffHeap(ByteBuffer keyValue, long reallocPtr) throws NativeMemoryException {
		keyValue.position(0);
		int keyLen = keyValue.getInt();
		int valLen  = keyValue.getInt();
		
		int total = toAllocate(keyLen, valLen);//8/*4+4*/+keyLen + valLen+OFFSET;
		long realAddress = getRealAddress(reallocPtr);
		//int allocd = NativeMemory.mallocUsableSize(realAddress);
		long putPtr; 
		boolean reuse = false;
		
		//if(allocd >= total){
		//	putPtr = realAddress; reuse = true;
		//} else{
			putPtr = NativeMemory.realloc(realAddress, total);
		//}
		if(putPtr == 0L) return 0L;
		IOUtils.putInt(putPtr, 0+OFFSET, keyLen);
		IOUtils.putInt(putPtr, 4+OFFSET, valLen);
		NativeMemory.memcpy(keyValue, 8, keyLen, putPtr, 8+OFFSET);
		NativeMemory.memcpy(keyValue, 8 + keyLen, valLen, putPtr, 8 + keyLen+OFFSET);
		
		//FIXME: if putPtr == realocPtr and reuse == false we unset nextPtr
		
		if(!reuse) IOUtils.putLong(putPtr, NP_OFFSET, 0L);
		return checkSetBit(reallocPtr, putPtr);
	}

	/**
	 * Copy to off heap recycle.
	 *
	 * @param keyValue the key value
	 * @param recPtr the rec ptr
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected long copyToOffHeapRecycle(ByteBuffer keyValue, long recPtr) throws NativeMemoryException {
		
		keyValue.position(0);
		int keyLen = keyValue.getInt();
		int valLen  = keyValue.getInt();		
		int required = toAllocate(keyLen, valLen);
		@SuppressWarnings("unused")
        long allocd = NativeMemory.mallocUsableSize(recPtr);
		// THIS works because we alwaus free/malloc in ralloc
		long putPtr = 
			NativeMemory.realloc(recPtr, required);	
		// Allocation failed?
		if(putPtr == 0L) return 0L;
		
		IOUtils.putInt(putPtr, 0+OFFSET, keyLen);
		IOUtils.putInt(putPtr, 4+OFFSET, valLen);
		NativeMemory.memcpy(keyValue, 8, keyLen, putPtr, 8+OFFSET);
		NativeMemory.memcpy(keyValue, 8 + keyLen, valLen, putPtr, 8 + keyLen+OFFSET);		
		IOUtils.putLong(putPtr, NP_OFFSET, 0L);
		
		return putPtr;
	}

	/**
	 * Copy to off heap.
	 *
	 * @param keyValue the key value
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	protected long copyToOffHeap(ByteBuffer keyValue) throws NativeMemoryException {
		keyValue.position(0);
		int keyLen = keyValue.getInt();
		int valLen  = keyValue.getInt();
		long putPtr = NativeMemory.malloc(8/*4+4*/+keyLen + valLen+OFFSET);
		// Allocation failed?
		if(putPtr == 0L) return 0L;
		
		IOUtils.putInt(putPtr, 0+OFFSET, keyLen);
		IOUtils.putInt(putPtr, 4+OFFSET, valLen);
		NativeMemory.memcpy(keyValue, 8, keyLen, putPtr, 8+OFFSET);
		NativeMemory.memcpy(keyValue, 8 + keyLen, valLen, putPtr, 8 + keyLen+OFFSET);
		
		IOUtils.putLong(putPtr, NP_OFFSET, 0L);
		return putPtr;
	}

	/**
	 * Gets value by key (w/o updating exp/usage).
	 *
	 * @param key the key
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object peek(Object key) throws NativeMemoryException, IOException
	{
		ByteBufferWithAddress bufa = getLocalBufferWithAddress();
		ByteBuffer buf = bufa.getBuffer();
		buf.clear();

		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		}else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);
		get(buf,  false);
		buf.position(0);
		int code = buf.getInt();
		if(code == 0) return null;
		if( code == -1) return Value.NULL;
		buf.position(12); // skip len + address
		if(isValueClassDefined()){
			return mSerializer.readCompressedWithClass(buf, /*mCodec,*/ getValueClass());
		} else{
			return mSerializer.readCompressed(buf/*, mCodec*/);
		}
	}	
	
	/**
	 * Gets value by key.
	 *
	 * @param key the key
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object get(Object key) throws NativeMemoryException, IOException
	{
		ByteBufferWithAddress bufa = getLocalBufferWithAddress();
		ByteBuffer buf = bufa.getBuffer();
		buf.clear();

		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		} else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);
		//get(buf);
		getNative(buf, bufa.getAddress());
		buf.position(0);
		int code = buf.getInt();
		if(code == 0) return null;
		if( code == -1) /*we have null value, but not null key*/ return Value.NULL;
		buf.position(12); // skip len + address
		//*DEBUG*/ System.out.println("compressed get size="+code);
		buf.limit(12 + code);
		if(isValueClassDefined()){
			return mSerializer.readCompressedWithClass(buf, /*mCodec,*/ getValueClass());
		} else{
			 return mSerializer.readCompressed(buf/*, mCodec*/);
		}
	}

	/**
	 * Gets the.
	 *
	 * @param key the key
	 * @param valueHolder the value holder
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object get(Object key, Object valueHolder) throws NativeMemoryException, IOException
	{
		ByteBufferWithAddress bufa = getLocalBufferWithAddress();
		ByteBuffer buf = bufa.getBuffer();
		buf.clear();

		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		} else{
			mSerializer.write(buf, key);
		}
		
		int pos = buf.position();
		buf.putInt(0, pos -4);

		getNative(buf, bufa.getAddress());
		buf.position(0);
		int code = buf.getInt();
		if(code == 0) return null;
		if( code == -1) /*we have null value, but not null key*/ return Value.NULL;
		buf.position(12); // skip len + address
		if(isValueClassDefined()){
			return mSerializer.readCompressedWithValueAndClass(buf, /*mCodec,*/ valueHolder, getValueClass());
		} else{
			return mSerializer.readCompressedWithValue(buf, /*mCodec,*/ valueHolder);
		}
	}
	
	/**
	 * Returns address of key-value record.
	 *
	 * @param key the key
	 * @return the address
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public long getAddress(ByteBuffer key) throws NativeMemoryException, IOException
	{

		long ptr = getNativePtr(key);
		if(ptr == 0) return 0;
		return ptr+OFFSET;
	}
	
	/**
	 * Returns szie of serialized value in bytes.
	 *
	 * @param key the key
	 * @return the value size
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public long getValueSize(Object key) throws NativeMemoryException, IOException
	{
		long ptr = getAddress(key);
		if(ptr <= 0) return NOT_FOUND;
		return IOUtils.getUInt(ptr, 4);
	}
	
	
	/**
	 * Returns szie of serialized value+key in bytes.
	 *
	 * @param key the key
	 * @return the key value size
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public long getKeyValueSize(Object key) throws NativeMemoryException, IOException
	{
		long ptr = getAddress(key);
		if(ptr <= 0) return NOT_FOUND;
		return IOUtils.getUInt(ptr, 4)+ IOUtils.getUInt(ptr, 0);
	}
	
	/**
	 * Returns memory occupied by a given 'key' (and corresponding value) in a cache.
	 *
	 * @param key the key
	 * @return total allocated memory
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public long getKeyValueAllocatedSize(Object key) throws NativeMemoryException, IOException
	{
		long ptr = getAddress(key);
		if(ptr <= 0) return NOT_FOUND;
		return NativeMemory.mallocUsableSize(ptr-OFFSET);
		
	}
	
	/**
	 * Returns address of key-value record.
	 *
	 * @param key the key
	 * @return the address
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public long getAddress(Object key) throws NativeMemoryException, IOException
	{
		ByteBuffer buf = getLocalBufferWithAddress().getBuffer();		
		long bufPtr = getLocalBufferWithAddress().getAddress();
		buf.clear();
		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		}else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);
		long ptr = getNativePtr(buf, bufPtr);
		if(ptr == 0) return 0;
		return ptr+OFFSET;
	}
	
	/**
	 * We do not have guarantee that we return all 'num' keys
	 * Usually it should work if 'num' << mTotalEntries
	 * @param num
	 * @return
	 * @throws NativeMemoryException
	 * @throws IOException
	 */
	
	public Object[] getRandomKeys(int num) throws NativeMemoryException, IOException
	{
		Object[] keys = new Object[num];
		Random r = new Random();
		// Index starts in a first half of a buckets
		int max = mBucketNumber / 2;
		int startIndex = r.nextInt(max);
		int endIndex = mBucketNumber -1;
		CacheScanner scanner = new CacheScanner(this, startIndex, endIndex, 0);
		int count = 0;
		while (scanner.hasNext() && count < num){
			keys[count++] = scanner.nextKey(); 
		}
		scanner.close();
		return keys;
	}
	
	/**
	 * 
	 * TODO: Test it
	 * Works well for LRU and LFU
	 * Rerturns
	 * Return >=0, if -1 - than NOT_FOUND
	 * @param key
	 * @return
	 * @throws IOException
	 * @throws NativeMemoryException 
	 */
	public long getEvictionData(Object key) throws IOException, NativeMemoryException
	{
		if(isRealEvictionPolicy() == false) return -1;
		ByteBuffer buf = getLocalBufferWithAddress().getBuffer();		
		//long bufPtr = getLocalBufferWithAddress().getAddress();
		buf.clear();
		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		}else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);	
		
		buf.position(0);
		int index = Math.abs(Utils.hash_murmur3(buf, 4, buf.getInt(), 0)) % mBucketNumber;

		// Get Bucket Read Lock
		SpinReadWriteLock lock = getLock(index);
		ReadLock readLock = null;
		if(lock != null){
			readLock = lock.readLock();
			readLock.lock();
		}
		try{

			long ptr = IOUtils.getLong(mMemPointer, ((long)index)*8);
			
			if(ptr == 0){
				buf.putInt(0,0);
				return -1; // NOT_FOUND
			} else{				
				buf.position(0);
				long resultPtr = getPtr(NativeMemory.getBufferAddress(buf), getRealAddress(ptr));
			
				if(resultPtr == 0) return -1; // NOT_FOUND

				long value = IOUtils.getUInt(resultPtr, 4);
				return mEvictionAlgo.translate(value);
			}
		}finally{
			if(readLock != null) readLock.unlock();
		}
		
	}
	
	/**
	 * Put.
	 *
	 * @param key the key
	 * @param value the value
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void put(Object key, Object value) throws NativeMemoryException, IOException
	{
		put(key, value, mExpireDefault);
	}

	 /**
   * Put with codec
   *
   * @param key the key
   * @param value the value
   * @throws NativeMemoryException the native memory exception
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void put(Object key, Object value, Codec codec) throws NativeMemoryException, IOException
  {
    put(key, value, codec, mExpireDefault);
  }
	
	/**
	 * Put if absent.
	 *
	 * @param key the key
	 * @param value the value
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object putIfAbsent(Object key, Object value) throws NativeMemoryException, IOException{
		return putIfAbsent(key, value, mExpireDefault);
	}
	
	/**
	 * Put if absent.
	 *
	 * @param key the key
	 * @param value the value
	 * @param expire the expire
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private Object putIfAbsent(Object key, Object value, int expire) throws NativeMemoryException, IOException {
		SpinReadWriteLock lock = getLockForKey(key);
		ReadLock readLock = null;
		if(lock != null){
			readLock = lock.readLock();
			readLock.lock();
		}
		try{
			Object val = get(key);
			if(val != null){
				return val;
			} else{
				put(key, value, expire);
			}
		}finally{
			if(readLock != null) readLock.unlock();
		}
		return null;
	}


	/**
	 * Put.
	 *
	 * @param key the key
	 * @param value the value
	 * @param expTime the exp time (use IMMORTAL if you want this item be pinned)
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void put(Object key, Object value, long expTime) throws NativeMemoryException, IOException
	{
		ByteBuffer buf = getLocalBufferWithAddress().getBuffer();
		
		buf.clear();

		buf.position(8);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		} else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -8);			
		buf.position(pos);			
		
		if(value != null){
			if(isValueClassDefined()){
				mSerializer.writeCompressedWithClass(buf, value, mCodec, getValueClass());
			} else{
				mSerializer.writeCompressed(buf, value, mCodec);
			}
			int pos2 = buf.position();
			buf.putInt(4, pos2-pos);
		} else{
			buf.putInt(4, 0);
		}
		put(buf, expTime);
		
	}

	 /**
   * Put with codec.
   *
   * @param key the key
   * @param value the value
   * @param expTime the exp time (use IMMORTAL if you want this item be pinned)
   * @throws NativeMemoryException the native memory exception
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void put(Object key, Object value, Codec codec, long expTime) throws NativeMemoryException, IOException
  {
    ByteBuffer buf = getLocalBufferWithAddress().getBuffer();
    
    buf.clear();

    buf.position(8);
    if(isKeyClassDefined()){
      mSerializer.writeWithClass(buf, key, getKeyClass());
    } else{
      mSerializer.write(buf, key);
    }
    int pos = buf.position();
    buf.putInt(0, pos -8);      
    buf.position(pos);      
    
    if(value != null){
      if(isValueClassDefined()){
        mSerializer.writeCompressedWithClass(buf, value, codec, getValueClass());
      } else{
        mSerializer.writeCompressed(buf, value, codec);
      }
      int pos2 = buf.position();
      buf.putInt(4, pos2-pos);
    } else{
      buf.putInt(4, 0);
    }
    put(buf, expTime);
    
  }
	
	public boolean touch(Object key) throws NativeMemoryException, IOException
	{
    ByteBuffer buf = getLocalBufferWithAddress().getBuffer();   
    long bufPtr = getLocalBufferWithAddress().getAddress();
    buf.clear();
    buf.position(4);
    if(isKeyClassDefined()){
      mSerializer.writeWithClass(buf, key, getKeyClass());
    } else{
      mSerializer.write(buf, key);
    }
    int pos = buf.position();
    buf.putInt(0, pos -4);      
    buf.position(pos);
    return touch(buf, bufPtr);
	}
	// TODO: tests for 'touch'
	private boolean touch(ByteBuffer key, long bufPtr) {
    key.position(0);
    int index = Math.abs(Utils.hash_murmur3(key, 4, key.getInt(), 0)) % mBucketNumber;

    // Get Bucket Read Lock
    SpinReadWriteLock lock = getLock(index);
    WriteLock writeLock = null;
    if(lock != null){
      writeLock = lock.writeLock();
      writeLock.lock();
    }
    try{
      long ptr = IOUtils.getLong(mMemPointer, ((long)index) * 8);
      if(ptr == 0){
        return false;
      } else{       
        long resultPtr = getPtr(bufPtr, getRealAddress(ptr));
        if(resultPtr == 0) return false;
        mEvictionAlgo.hitEntry(resultPtr, lock != null? lock.getSpinLock(): null);
        return true;
      }
    }finally{
      if(writeLock != null) writeLock.unlock();

    }
  }

  public boolean pin(Object key) throws NativeMemoryException, IOException
	{
		ByteBuffer buf = getLocalBufferWithAddress().getBuffer();		
		long bufPtr = getLocalBufferWithAddress().getAddress();
		buf.clear();
		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		} else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);			
		buf.position(pos);
		// We have key serialized
		return pin(buf, bufPtr);
	}
	
	public boolean pin(ByteBuffer key)  throws NativeMemoryException, IOException
	{
		return pin(key, NativeMemory.getBufferAddress(key));
	}
	
	public boolean pin(ByteBuffer key, long bufPtr)  throws NativeMemoryException, IOException
	{
		key.position(0);
		int index = Math.abs(Utils.hash_murmur3(key, 4, key.getInt(), 0)) % mBucketNumber;

		// Get Bucket Read Lock
		SpinReadWriteLock lock = getLock(index);
		WriteLock writeLock = null;
		if(lock != null){
			writeLock = lock.writeLock();
			writeLock.lock();
		}
		try{
			long ptr = IOUtils.getLong(mMemPointer, ((long)index)*8);//mBuffer.get(index);
			
			if(ptr == 0){
				return false;
			} else{				
				long resultPtr = getPtr(bufPtr, getRealAddress(ptr));
				if(resultPtr == 0) return false;
				IOUtils.putInt(resultPtr, 0, IMMORTAL);
				return true;
			}
		}finally{
			if(writeLock != null) writeLock.unlock();

		}
	}
	/**
	 * To allocate.
	 *
	 * @param keyLength the key length
	 * @param valueLength the value length
	 * @return the int
	 */
	protected final int toAllocate(final int keyLength, final int valueLength )
	{
		return OFFSET+8 +keyLength+valueLength;
	}
	
	/**
	 * To allocate.
	 *
	 * @param keyVal the key val
	 * @return the int
	 */
	protected final int toAllocate(ByteBuffer keyVal)
	{
		keyVal.position(0);
		int ks = keyVal.getInt();
		int vs = keyVal.getInt();
		return OFFSET+8 + ks+ vs;
	}
	

	
	/**
	 * Removes the value.
	 *
	 * @param key the key
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object removeValue(Object key) throws NativeMemoryException, IOException
	{
		ReentrantReadWriteLock lock = getLockForKey(key);
		WriteLock writeLock = null;
		if(lock != null){
			writeLock = lock.writeLock();
			writeLock.lock();
		}
		
		try{
			Object value = get(key);			
			if(value == null) return null;
			remove(key);
			return value;
		}finally{
			if(writeLock != null) writeLock.unlock();
		}
		
	}
	
	/**
	 * TODO: test case
	 * Evict forcefully 'total' objects
	 * using current eviction algoritm.
	 * The number MUST be relatively small
	 * @param total - number of objects to evict
	 * @return total size of deallocated memory
	 * @throws NativeMemoryException the native memory exception
	 */
	public long evictObjects(int total) throws NativeMemoryException
	{
		if(isLockFreeMode()){
			// TODO ??? why it is not supported
			throw new RuntimeException("not supported in LockFree mode");
		}
		

		EvictionAlgo algo = getEvictionAlgo();
		if(algo == null || algo instanceof EvictionNone){
			throw new RuntimeException("no eviction enabled.");
			
		}
		
		long deallocated = 0L;
		
		int stripe = (int) (mForcedEvictionCounter.incrementAndGet() % LOCK_STRIPES);
		SpinReadWriteLock lock = null;
		WriteLock writeLock = null;
		try{
		
			do{ // scan through stripes and try to lock one
				lock = getLock(stripe);
				writeLock = lock.writeLock();
				if(writeLock.tryLock()) break;
				stripe = (stripe+1) % LOCK_STRIPES;
			} while(true);
		
			// current stripe
			// we need to find random 'total*EVICTION_CANDIDATE_LIST_SIZE' objects
			// and select 'total' out of them
			
			int listSize = total*mEvCandidateListSize;
			
			EvictionData data = new EvictionData(total*mEvCandidateListSize, stripe);
			int startIndex = this.mBucketWidth * stripe;
			int endIndex   = this.mBucketWidth * (stripe+1) -1;
			int totalAdded =0;
			int index= startIndex;
		
			while(index <= endIndex && totalAdded < listSize)
			{
				long rawPtr =IOUtils.getLong(mMemPointer, ((long)index) * 8);
				if( rawPtr != 0){
					long ptr = getRealAddress(rawPtr);
					long expire = IOUtils.getUInt(ptr, 0);
					// Do not put pinned objects into eviction candidate list
					if(expire == IMMORTAL) {
						index++;
						continue;
					}
					data.addCandidate(index, expire, IOUtils.getUInt(ptr, 4));
					totalAdded++;
				}
				index++;
			}
			// Now we have a list, but list can be incomplete < 'total' 			

			long evictedPtr = 0;
			int totalEvicted = 0;
			while (totalEvicted++ < total){
				evictedPtr = algo.doEviction(data);
				if(evictedPtr == 0){
					throw new RuntimeException("unexpected evicted ptr - NULL");
				}
				deallocated += NativeMemory.mallocUsableSize(evictedPtr);
			}
			updateAllocatedMemory(-deallocated);
			return deallocated;
		}finally{
			if(writeLock!= null) writeLock.unlock();
		}
	}
	
	
	/**
	 * Evict multiple objects.
	 *
	 * @param total the total
	 * @return the long
	 * @throws NativeMemoryException the native memory exception
	 */
	public long evictMultipleObjects(int total) throws NativeMemoryException
	{
		long freedMem = 0;
		
		for(int i = 0; i < total; i++)
		{
			freedMem += evictObject();
		}
		return freedMem;
		
	}
	
	/**
	 * TODO: test case
	 * Evict forcefully 'total' objects
	 * using current eviction algoritm.
	 * The number MUST be relatively small
	 *
	 * @return total size of deallocated memory
	 * @throws NativeMemoryException the native memory exception
	 */
	public long evictObject() throws NativeMemoryException
	{
		if(isLockFreeMode()){
			// TODO ??? why it is not supported
			throw new RuntimeException("not supported in LockFree mode");
		}
		

		EvictionAlgo algo = getEvictionAlgo();
		if(algo == null || algo instanceof EvictionNone){
			throw new RuntimeException("no eviction enabled.");
			
		}
		
		long deallocated = 0L;
		
		int stripe = (int) (mForcedEvictionCounter.incrementAndGet() % LOCK_STRIPES);
		SpinReadWriteLock lock = null;
		WriteLock writeLock = null;
		try{
		
			do{ // scan through stripes and try to lock one
				lock = getLock(stripe);
				writeLock = lock.writeLock();
				if(writeLock.tryLock()) break;
				stripe = (stripe+1) % LOCK_STRIPES;
			} while(true);
		
			// current stripe
			// we need to find random 'total*EVICTION_CANDIDATE_LIST_SIZE' objects
			// and select 'total' out of them
			
			int listSize = mEvCandidateListSize;
			
			EvictionData data = new EvictionData(mEvCandidateListSize, stripe);
			int startIndex = this.mBucketWidth * stripe;
			int endIndex   = this.mBucketWidth * (stripe+1) -1;
			int totalAdded =0;
			int index= startIndex;
		
			while(index <= endIndex && totalAdded < listSize)
			{
				long rawPtr = IOUtils.getLong(mMemPointer, ((long)index) * 8);
				if( rawPtr != 0){
					long ptr = getRealAddress(rawPtr);
					long expire = IOUtils.getInt(ptr, 0);
					// Do not put pinned objects into eviction candidate list
					if(expire == IMMORTAL){
						index++;continue;
					}
					if(data.addCandidate(index, expire, IOUtils.getUInt(ptr, 4))){
					  totalAdded++;
					}
				}
				index++;
			}
			// Now we have a list, but list can be incomplete < 'total' 			

			long evictedPtr = 0;
			int totalEvicted = 0;
			while (totalEvicted++ < 1){
				evictedPtr = algo.doEviction(data);
				if(evictedPtr != 0L){									
				  deallocated += NativeMemory.mallocUsableSize(evictedPtr);
				}
			}
			updateAllocatedMemory(-deallocated);
			return deallocated;
		}finally{
			if(writeLock!= null) writeLock.unlock();
		}
	}
	
	
	/**
	 * Removes the value by a given key.
	 * TODO: EVICTION data are bare pointers - we can evict (delete) object twice
	 * @param key the key
	 * @return true, if successful
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean remove(Object key) throws NativeMemoryException, IOException
	
	{
		ByteBuffer buf = getLocalBufferWithAddress().getBuffer();		
		buf.clear();
		buf.position(4);
		if(isKeyClassDefined()){
			mSerializer.writeWithClass(buf, key, getKeyClass());
		} else{
			mSerializer.write(buf, key);
		}
		int pos = buf.position();
		buf.putInt(0, pos -4);
		
		return remove(buf);
	}
	
	
	
	/**
	 * Removes the key.
	 *
	 * @param key the key
	 * @return the byte[]
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean remove(ByteBuffer key) throws NativeMemoryException {

		if(key == null) {
			LOG.warn("null key");
			return false;// we do not support NULLs yet
		}
		key.position(0);
		int keyLen = key.getInt();
		int index = Math.abs(Utils.hash_murmur3(key, 4, keyLen, 0)) % mBucketNumber;
		
		// Get Bucket Read Lock
		ReentrantReadWriteLock lock = getLock(index);
		WriteLock writeLock = null;
		if(lock != null){
			writeLock = lock.writeLock();
			writeLock.lock();
		}
		
		try{
			long ptr = IOUtils.getLong(mMemPointer, ((long)index) * 8);//mBuffer.get(index);
			long realPtr = getRealAddress(ptr);
			if(ptr == 0) {
				return false;

			} else{
				// scan the list; find the key (realloc) or append to 
				// OK, ptr != null get the key and compare with key
				int otherKeySize = IOUtils.getInt(realPtr, 0+OFFSET);
				boolean equalSize = (otherKeySize == keyLen);
				//byte[] otherKey = NativeMemory.memread2a(realPtr, 8+OFFSET, keySize);
				if(equalSize && Utils.equals(key, 4, keyLen, NativeMemory.memread2ea(realPtr, 
						8 + OFFSET, otherKeySize, getLocalKeyStore(otherKeySize)))){
					long nextPtr = getNextAddressRaw(realPtr);
					long size = NativeMemory.mallocUsableSize(realPtr);
					NativeMemory.free(realPtr);
					//mBuffer.put(index, nextPtr);// even if nextPtr == 0 we are fine
					IOUtils.putLong(mMemPointer, ((long)index) * 8, nextPtr);
					// Decrement total count
					mTotalItems.decrementAndGet();
					//mMemorySize.addAndGet(-size);
					updateAllocatedMemory(-size);
					return true;
				} else{
					long nextPtr = getNextAddressRaw(ptr);
					if(nextPtr != 0){
						return findAndRemove(ptr, nextPtr, key);
					} else{
						return false;
					}
				}
			}
		} finally{
			if(writeLock != null) writeLock.unlock();
		}
	}

	
	/**
	 * Find and remove.
	 *
	 * @param prevPtr the prev ptr
	 * @param ptr the ptr
	 * @param key the key
	 * @return true, if successful
	 * @throws NativeMemoryException the native memory exception
	 */
	private boolean findAndRemove(long prevPtr, long ptr, ByteBuffer key) throws NativeMemoryException {
		long realPtr = getRealAddress(ptr);
		if(ptr == 0L){
			// nothing found
			return false;
		} else{
			int otherKeySize = IOUtils.getInt(realPtr, 0+OFFSET);
			key.position(0);
			int keyLen = key.getInt();
			//byte[] otherKey = NativeMemory.memread2a(realPtr, 8+OFFSET, keySize);
			boolean equalSize = (otherKeySize == keyLen);
			if(equalSize && Utils.equals(key, 4, keyLen, NativeMemory.memread2ea(realPtr, 
					8 + OFFSET, otherKeySize, getLocalKeyStore(otherKeySize)))){
	   			long nextPtr = getNextAddress(realPtr);//IOUtils.getLong(ptr, NP_OFFSET);
				setNextPtr(getRealAddress(prevPtr), nextPtr);				
                long size = NativeMemory.mallocUsableSize(realPtr);
                NativeMemory.free(realPtr);
				// Decrement total count
				mTotalItems.decrementAndGet();
				//mMemorySize.addAndGet(-size);
				updateAllocatedMemory(-size);
				return true;
			} else{
				// recursion
				long nextPtr = getNextAddressRaw(realPtr);
				return findAndRemove(ptr, nextPtr, key);
			}
		}

	}

	/**
	 * Gets the disk store.
	 *
	 * @return the disk store
	 */
	public DiskStore getDiskStore()
	{
		return mDiskStore;
	}
	
	/**
	 * Sets the disk store.
	 *
	 * @param ds the new disk store
	 */
	void setDiskStore(DiskStore ds)
	{
		this.mDiskStore = ds;
		if(ds != null){
		  mIsPersistent = true;
		  mPersistenceType = ds.getConfig().getPersistenceMode();
		}
		
	}
	
	/**
	 * Shutdown.
	 *
	 * @throws KodaException the koda exception
	 */
	public void shutdown() throws KodaException
	{
		LOG.info("Shutting down cache : "+mCacheConfig.getQualifiedName()+" started at :"+(new Date()));
    LOG.info("Size: "+ size() +" Total memory: "+ getTotalAllocatedMemorySize());
		
		// shutdown executor
		
		if(mIsPersistent){
			disableCache();		
			try{
				if(mPersistenceType == PersistenceMode.ONDEMAND ||
						mPersistenceType == PersistenceMode.SNAPSHOT)
				{
					save();
				} else if(mPersistenceType != PersistenceMode.NONE){
					// WRITE_BEHIND or WRITE_THROUGH
					mDiskStore.flush();
				
				}
				mDiskStore.close();
			}catch(IOException e){
				throw new KodaException(e);
			}
		}
		LOG.info("Shutting down cache : "+mCacheConfig.getQualifiedName()+" finished at "+(new Date()));	
	}

	
	/**
	 * Is cache disabled.
	 *
	 * @return tru or false
	 */
	public boolean  isDisabled() {
		return mIsDisabled.get();
	}


	
	/**
	 * Disable cache.
	 */
	private void disableCache()  {
		// Acquiring all write locks
		LOG.info("Acquiring exclusive cache lock for "+mCacheConfig.getQualifiedName());
		
		for(int i =0; i < mLocks.length; i++){
			// lock stripes one-by-one
			mLocks[i].writeLock().lock();
		}
		//TODO we need to notify all waiting threads that cache is disabled
		mIsDisabled.set(true);
		LOG.info("Exclusive cache lock for "+mCacheConfig.getQualifiedName()+" was acquired. Cache disabled.");
	}

	/**
	 * Save.
	 *
	 * @throws KodaException the koda exception
	 */
	public void save() throws KodaException{
		if(mDiskStore == null) 
			throw new KodaException("disk store is not enabled");
		
		save(mDiskStore, true, null);
	}
	
	
	/**
	 * Save.
	 *
	 * @param pl the pl
	 * @throws KodaException the koda exception
	 */
	public void save(ProgressListener pl) throws KodaException{
		if(mDiskStore == null) 
			throw new KodaException("disk store is not enabled");
		
		save(mDiskStore, true, pl);
	}	
	
	/**
	 * Save.
	 *
	 * @param store the store
	 * @param ignoreExpired the ignore expired
	 * @param pl the pl
	 * @throws KodaException the koda exception
	 */
	public synchronized void save(DiskStore store, boolean ignoreExpired, ProgressListener pl) throws KodaException
	{
		
		if(store == null){
			if(mDiskStore == null){
				throw new KodaException("disk store is not enabled");
			} else{
				store = mDiskStore;
			}
		}
		
		try{
			// TODO callback
			if(store instanceof RawFSStore){
				((RawFSStore) store).storeFast(this, ignoreExpired, pl);
			} else{
				store.store(this, ignoreExpired, pl);
			}
		}catch(Exception e){
			LOG.error(e);
			throw new KodaException(e);
		}
	}
	
	/**
	 * Merge stripe index into ptr.
	 *
	 * @param ptr the ptr
	 * @param stripeNumber the stripe number
	 * @return the long
	 */
	protected final long mergeStripeIndexIntoPtr(long ptr, int stripeNumber)
	{
		//TODO This method relies on fact that we have 256 stripes by default
		return ptr | (((long) stripeNumber ) << 56);
	}
	
	/**
	 * Gets the real ptr.
	 *
	 * @param ptr the ptr
	 * @return the real ptr
	 */
	public static final long getRealPtr(long ptr)
	{
		return (ptr & 0x00ffffffffffffffL);
	}
	
	/**
	 * Gets the key.
	 *
	 * @param ptr the ptr
	 * @param buf the buf
	 * @return the key
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static void getKey(long ptr, ByteBuffer buf) 
	{
		long bufptr = NativeMemory.getBufferAddress(buf);
		getKeyDirect(ptr, bufptr);
	}
		
	/**
	 * Gets the key direct.
	 *
	 * @param ptr the ptr
	 * @param bufPtr the buf ptr
	 * @return the key direct
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static void getKeyDirect(long ptr, long bufPtr) {
		//final int off = OFFSET;
		// ptr - is pointer to malloc area (not real address)
		long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int size = unsafe.getInt(mem + OFFSET);
		unsafe.putInt(bufPtr, size);
		unsafe.copyMemory(mem+ OFFSET + 8, bufPtr + 4, size);		
		NativeMemory.unlockAddress(ptr);
	}
	
	
	/**
	 * Gets the key direct bytes.
	 *
	 * @param ptr the ptr
	 * @param buffer the buffer
	 * @return the key direct bytes
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static int getKeyDirectBytes(long ptr, byte[] buffer) 
	{
		//final int off = OFFSET;
		// ptr - is pointer to malloc area (not real address)
		long mem = NativeMemory.lockAddress(ptr);		
		Unsafe unsafe = NativeMemory.getUnsafe();
		int size = unsafe.getInt(mem + OFFSET);
		if( size > buffer.length){
			return -1;
		}		
		unsafe.copyMemory(null, mem + OFFSET + 8 , buffer, org.yamm.util.Utils.BYTE_ARRAY_OFFSET, size);		
		NativeMemory.unlockAddress(ptr);
		return size;
	}
	/**
	 * Gets the key direct new bytes.
	 *
	 * @param ptr the ptr
	 * @return the key direct new bytes
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static byte[] getKeyDirectNewBytes(long ptr)
	{
		long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int size = unsafe.getInt(mem + OFFSET);
		byte[] buf = new byte[size];		
		NativeMemory.memcpy(mem, OFFSET + 8, buf, 0, size);		
		NativeMemory.unlockAddress(ptr);
		return buf;
	}
	
	/**
	 * Gets the record.
	 *
	 * @param ptr the ptr
	 * @param buf the buf
	 * @return the record
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static  void getRecord(long ptr, ByteBuffer buf) 
	{
		long bufptr = NativeMemory.getBufferAddress(buf);
		getRecordDirect(ptr, bufptr);
	}
	
	/**
	 * Gets the record direct.
	 *
	 * @param ptr the ptr
	 * @param bufPtr the buf ptr
	 * @return the record direct
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static void getRecordDirect(long ptr, long bufPtr)
	{
		final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int ksize = unsafe.getInt(mem + OFFSET);
		int vsize = unsafe.getInt(mem + OFFSET + 4);
		
		unsafe.copyMemory(mem + OFFSET, bufPtr, ksize + vsize + 8);
		
		NativeMemory.unlockAddress(ptr);
	}
	
	/**
	 * Gets the record direct bytes.
	 *
	 * @param ptr the ptr
	 * @param buffer the buffer
	 * @return the record direct bytes
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static int getRecordDirectBytes(long ptr, byte[] buffer) 
	{
		final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int ksize = unsafe.getInt(mem + OFFSET);
		int vsize = unsafe.getInt(mem + OFFSET + 4);
		if( ksize + vsize + 8 > buffer.length){
			return -1;
		}
		unsafe.copyMemory(null, mem + OFFSET, buffer, org.yamm.util.Utils.BYTE_ARRAY_OFFSET, ksize + vsize + 8);
		
		NativeMemory.unlockAddress(ptr);
		return ksize + vsize + 8;
	}
	
	/**
	 * Gets the record direct new bytes.
	 *
	 * @param ptr the ptr
	 * @return the record direct new bytes
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static  byte[] getRecordDirectNewBytes(long ptr)
	{
		final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int ksize = unsafe.getInt(mem + OFFSET);
		int vsize = unsafe.getInt(mem + OFFSET + 4);
		byte[] buffer = new byte[ksize + vsize + 8];
		unsafe.copyMemory(null, mem + OFFSET, buffer, org.yamm.util.Utils.BYTE_ARRAY_OFFSET, ksize + vsize + 8);		
		NativeMemory.unlockAddress(ptr);
		return buffer;
	}
	
	
	/**
	 * Gets the value.
	 *
	 * @param ptr the ptr
	 * @param buf the buf
	 * @return the value
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static  void getValue(long ptr, ByteBuffer buf)
	{
		long bufptr = NativeMemory.getBufferAddress(buf);
		getValueDirect(ptr, bufptr);
	}
	
	/**
	 * Gets the value direct.
	 *
	 * @param ptr the ptr
	 * @param bufPtr the buf ptr
	 * @return the value direct
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static void getValueDirect(long ptr, long bufPtr) 
	{
		final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int ksize = unsafe.getInt(mem + OFFSET);
		int vsize = unsafe.getInt(mem + OFFSET + 4);
		unsafe.putInt(bufPtr, vsize);
		unsafe.copyMemory(mem + OFFSET + 8 + ksize, bufPtr + 4, vsize);
		NativeMemory.unlockAddress(ptr);
	}
	
	/**
	 * Gets the value direct bytes.
	 *
	 * @param ptr the ptr
	 * @param buffer the buffer
	 * @return the value direct bytes
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static int getValueDirectBytes(long ptr, byte[] buffer) 
	{
		final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int ksize = unsafe.getInt(mem + OFFSET);
		int vsize = unsafe.getInt(mem + OFFSET + 4);
		if( vsize > buffer.length){
			return -1;
		}
		unsafe.copyMemory(null, mem + OFFSET + 8 + ksize, buffer,org.yamm.util.Utils.BYTE_ARRAY_OFFSET, vsize);
		NativeMemory.unlockAddress(ptr);
		return vsize;
	}
	
	/**
	 * Gets the value direct new bytes.
	 *
	 * @param ptr the ptr
	 * @return the value direct new bytes
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static byte[] getValueDirectNewBytes(long ptr) 
	{
		final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();
		int ksize = unsafe.getInt(mem + OFFSET);
		int vsize = unsafe.getInt(mem + OFFSET + 4);
		byte[] buffer = new byte[vsize];		
		NativeMemory.memcpy( mem , OFFSET + 8 + ksize, buffer, 0,  vsize);		
		NativeMemory.unlockAddress(ptr);
		return buffer;
	}

      	

	
	/**
	 * Gets the value into a buffer.
	 *
	 * @param key the key
	 * @param ptr the ptr
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static void get(ByteBuffer key, long ptr) 
	{

		long bufPtr = NativeMemory.getBufferAddress(key);		
		getIntoBufferPtr(bufPtr, ptr);
	}
	
	
	private static void getValue(long ptr, long bufPtr, int keySize) 
	{
		
		Unsafe unsafe = NativeMemory.getUnsafe();
		try {
			long mem = NativeMemory.lockAddress(ptr);
			if (mem == 0) {
				// not found
				unsafe.putInt(bufPtr, 0);
				return;
			}

			// long memptr = mem;

			int otherKeyLen = unsafe.getInt(mem + OFFSET);
			int otherValueLen = unsafe.getInt(mem + OFFSET + 4);

			if (otherKeyLen == keySize
					&& org.yamm.util.Utils.memcmp(mem + 8 + OFFSET, bufPtr + 4,
							keySize) == 0) {
				// keys are equals

				unsafe.putInt(bufPtr, otherValueLen > 0 ? otherValueLen : -1);
				unsafe.putLong(bufPtr + 4, mem);
				unsafe.copyMemory(mem + OFFSET + 8 + otherKeyLen,
						bufPtr + 4 + 8, otherValueLen);

			} else {
				// get next ptr
				mem += OFFSET - 8;
				long nextPtr = unsafe.getLong(mem) & 0x7fffffffffffffffL;
				getValue(nextPtr, bufPtr, keySize);
			}
		} finally {
			NativeMemory.unlockAddress(ptr);
		}
	}

	/**
	 * Gets the into buffer ptr.
	 *
	 * @param bufPtr the buf ptr
	 * @param ptr the ptr
	 * @return the into buffer ptr
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static void getIntoBufferPtr(long bufPtr, long ptr) 
	{
		//final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();	
		// ptr+4
		int size = unsafe.getInt(bufPtr);			
		getValue( ptr /*src*/, bufPtr /*dst*/, size);
		//NativeMemory.unlockAddress(ptr);		
	}
	/**
	 * Gets the ptr.
	 *
	 * @param bufPtr the buf ptr
	 * @param ptr the ptr
	 * @return the ptr
	 * @throws NativeMemoryException the j emalloc exception
	 */
	static long getPtr(long bufPtr, long ptr) {
		//final long mem = NativeMemory.lockAddress(ptr);
		Unsafe unsafe = NativeMemory.getUnsafe();	
		// ptr+4
		int size = unsafe.getInt(bufPtr);			
		long vptr = getValuePtr( ptr, bufPtr , size);
		//NativeMemory.unlockAddress(ptr);
		return vptr;
	}
	
	static long getRecordPtr(long bufPtr, long ptr) {

		Unsafe unsafe = NativeMemory.getUnsafe();	
		// ptr+4
		int size = unsafe.getInt(bufPtr);			
		long vptr = getRecordPtr( ptr, bufPtr , size);

		return vptr;
	}

	private static long  getRecordPtr(long ptr, long bufPtr, int keySize) 
	{
		Unsafe unsafe = NativeMemory.getUnsafe();
		try {
			long mem = NativeMemory.lockAddress(ptr);
			if (mem == 0) {
				// not found
				unsafe.putInt(bufPtr, 0);
				return 0;
			}

			long memptr = mem;

			int otherKeyLen = unsafe.getInt(memptr + OFFSET);

			if (otherKeyLen == keySize
					&& org.yamm.util.Utils.memcmp(memptr + 8 + OFFSET,
							bufPtr + 4, keySize) == 0) {
				// keys are equals
				return memptr;
			} else {
				// get next ptr
				memptr += OFFSET - 8;
				long nextPtr = unsafe.getLong(memptr) & 0x7fffffffffffffffL;
				return getValuePtr(nextPtr, bufPtr, keySize);
			}
		} finally {
			NativeMemory.unlockAddress(ptr);
		}
	}	
	
	private static long  getValuePtr(long ptr, long bufPtr, int keySize) 
	{
		Unsafe unsafe = NativeMemory.getUnsafe();
		try {
			long mem = NativeMemory.lockAddress(ptr);
			if (mem == 0) {
				// not found
				unsafe.putInt(bufPtr, 0);
				return 0;
			}

			long memptr = mem;

			int otherKeyLen = unsafe.getInt(memptr + OFFSET);

			if (otherKeyLen == keySize
					&& org.yamm.util.Utils.memcmp(memptr + 8 + OFFSET,
							bufPtr + 4, keySize) == 0) {
				// keys are equals
				return memptr;
			} else {
				// get next ptr
				memptr += OFFSET - 8;
				long nextPtr = unsafe.getLong(memptr) & 0x7fffffffffffffffL;
				return getRecordPtr(nextPtr, bufPtr, keySize);
			}
		} finally {
			NativeMemory.unlockAddress(ptr);
		}
	}
	
	public CodecType getCodecTypeForRecord(long ptr)
	{
	  Unsafe unsafe = NativeMemory.getUnsafe();
	  long realPtr = NativeMemory.lockAddress(ptr);
	  try{
	    int keySize = unsafe.getInt(realPtr + OFFSET);
	    int valueOffset = OFFSET + 8 + keySize;
	    // read 5-th byte
	    int type = unsafe.getByte( realPtr + valueOffset + 4);
	    return CodecType.values()[type];
	  } finally{
	    NativeMemory.unlockAddress(ptr);
	  }
	}

}

