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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import org.apache.log4j.Logger;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

import com.koda.KodaException;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionAlgo;

import com.koda.integ.ispn.OffHeapDataContainer;
import com.koda.io.serde.Serializer;
import com.koda.io.serde.Writable;
import com.koda.util.Utils;


@SuppressWarnings("unused")
// TODO: Auto-generated Javadoc
/**
 * The Class OffHeapHashMapPerfTest.
 */
public class PerfTest {

	/**
	 * The Enum CacheType.
	 */
	static enum CacheType {

		/** The KODE. */
		KODA,
		/** The EHCACHE. */
		EHCACHE,
		/** The EHCACH e_ bm. */
		EHCACHE_BM,
		/** The INFINISPAN. */
		INFINISPAN, 
 /** The INFINISPA n_ koda. */
 INFINISPAN_KODA,
		/** The HAZELCAST. */
		HAZELCAST,
		/** The COHERENCE. */
		COHERENCE,
		/** The GIGASPACES. */
		GIGASPACES,
		/** The XSCALE. */
		XSCALE
	}

	/** The Constant THREADS. */
	private final static String THREADS = "-t";

	/** The Constant BUCKETS. */
	private final static String BUCKETS = "-b"; // kode specific

	/** The Constant MAXMEMORY. */
	private final static String MAXMEMORY = "-mm";

	/** The Constant MAXITEMS. */
	private final static String MAXITEMS = "-mi";

	/** The Constant WRITE_RATIO. */
	private final static String WRITE_RATIO = "-writes"; // write ops %%

	/** The Constant CACHE. */
	private final static String CACHE = "-cache";

	/** The Constant DURATION. */
	private final static String DURATION = "-duration";
	
	private final static String EVICTION = "-eviction";
	
	private final static String EVICTION_LRU2Q = "lru2q";
	
	private final static String LRU2Q_INSERT_POINT = "-lru2qip";

	/** The Constant BIGMEMORY_SIZE. */
	private final static String BIGMEMORY_SIZE = "-bmsize";

	/** Cache implementation. */
	private final static String KODE = "kode";

	/** The Constant EHCACHE. */
	private final static String EHCACHE = "ehcache";

	/** The Constant EHCACHE_BM. */
	private final static String EHCACHE_BM = "ehcache_bm"; // BigMemory

	/** The Constant INFINISPAN. */
	private final static String INFINISPAN = "ispn"; // JBoss cache

	/** The Constant INFINISPAN_KODA. */
	private final static String INFINISPAN_KODA = "ispn_koda";

	/** The Constant HAZELCAST. */
	private final static String HAZELCAST = "hazelcast";

	/** The Constant COHERENCE. */
	private final static String COHERENCE = "coherence";

	/** The Constant GIGASPACES. */
	private final static String GIGASPACES = "gigaspaces";

	/** The Constant XSCALE. */
	private final static String XSCALE = "xscale"; // IBM extreme scale

	/** The N. */
	public static int N = 1024 * 1024;

	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(PerfTest.class);

	/** The s cache type. */
	private static CacheType sCacheType = CacheType.KODA;

	/** The Kode cache instance. */
	private static OffHeapCache mCache;

	/** The Ehcache cache manager. */
	private static CacheManager sEhcacheManager;

	/** The Ehcache cache. */
	private static Cache sEhcacheCache;

	/** Infinispan cache instance. */
	private static org.infinispan.Cache<byte[], byte[]> ispnCache;
	
	/** Infinispan cache instance. */
	private static org.infinispan.Cache<byte[], byte[]> ispnCacheKoda;
	/** The s test time. */
	private static long sTestTime = 600000;// 600 secs

	/** The s write ratio. */
	private static float sWriteRatio = 0.1f; // 10% puts - 90% gets

	/** The s interval. */
	private static long sInterval = 5000;

	/** Memory limit in bytes - used for Koda. */
	private static long sMemoryLimit = 1000000000L; // 1G by default

	/** Cache items limit - used for all other caches. */
	private static long sCacheItemsLimit = 0; // 1.5 M by default

	/** Number of client threads. */
	private static int sClientThreads = 2; // by default
	
	private static String sEvictionAlgo = "LRU";
	
	private static float sLru2qip= 0.5f;

	/** BigMemory size. */
	private static String sBigMemorySize = "1G"; // by default

	/** The s puts. */
	private static AtomicLong sPuts = new AtomicLong(0);

	/** The s gets. */
	private static AtomicLong sGets = new AtomicLong(0);

	/** The s in cache. */
	private static AtomicLong sInCache = new AtomicLong(0);

	/**
	 * Gets the key.
	 * 
	 * @param i
	 *            the i
	 * @param key
	 *            the key
	 * @return the key
	 */
	static final byte[] getKey(int i, byte[] key) {
		key[0] = (byte) (i >>> 24);
		key[1] = (byte) (i >>> 16);
		key[2] = (byte) (i >>> 8);
		key[3] = (byte) (i);
		return key;
	}

	/**
	 * Gets the key long.
	 * 
	 * @param i
	 *            the i
	 * @param key
	 *            the key
	 * @return the key long
	 */
	static final byte[] getKeyLong(long i, byte[] key) {
		key[0] = (byte) (i >>> 56);
		key[1] = (byte) (i >>> 48);
		key[2] = (byte) (i >>> 40);
		key[3] = (byte) (i >>> 32);
		key[4] = (byte) (i >>> 24);
		key[5] = (byte) (i >>> 16);
		key[6] = (byte) (i >>> 8);
		key[7] = (byte) (i);
		return key;
	}

	/**
	 * Gets the value copy.
	 * 
	 * @param v
	 *            the v
	 * @return the value copy
	 */
	static final byte[] getValueCopy(byte[] v) {
		byte[] value = new byte[v.length];
		System.arraycopy(v, 0, value, 0, v.length);
		return value;
	}

	/**
	 * Gets the key long copy.
	 * 
	 * @param i
	 *            the i
	 * @param key
	 *            the key
	 * @return the key long copy
	 */
	static final byte[] getKeyLongCopy(long i, byte[] key) {
		byte[] buf = new byte[key.length];
		System.arraycopy(key, 0, buf, 0, key.length);
		buf[0] = (byte) (i >>> 56);
		buf[1] = (byte) (i >>> 48);
		buf[2] = (byte) (i >>> 40);
		buf[3] = (byte) (i >>> 32);
		buf[4] = (byte) (i >>> 24);
		buf[5] = (byte) (i >>> 16);
		buf[6] = (byte) (i >>> 8);
		buf[7] = (byte) (i);
		return buf;
	}

	/**
	 * Gets the string.
	 *
	 * @param arr the arr
	 * @return the string
	 */
	static String getString(byte[] arr){
		StringBuffer sb = new StringBuffer();
		for(int i=0; i < arr.length; i++)
		{
			sb.append(arr[i]);
			if(i < arr.length -1) sb.append(",");
		}
		return sb.toString();
	}
	
	/**
	 * Gets the key long copy.
	 * 
	 * @param i
	 *            the i
	 * @param suffix
	 *            the suffix
	 * @return the key long copy
	 */
	static final String getKeyLongCopy(long i, String suffix) {
		return Long.toString(i) + suffix;
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public final static void main(String[] args) throws Exception {

		parseArgs(args);
		initCache();

		String[] keyPrefix = new String[sClientThreads];
		Random r = new Random();
		for (int i = 0; i < sClientThreads; i++) {
			keyPrefix[i] = "Thread[" + r.nextInt(1024 * 1024) + "]";
		}

		int opNumber = N / sClientThreads;

		long t1 = System.currentTimeMillis();
		ExecuteThread[] threads = startTest(keyPrefix, sClientThreads, opNumber);
		StatsCollector collector = new StatsCollector(sInterval, threads);
		LOG.info("Test started");
		collector.start();
		waitToFinish(threads);

		long t2 = System.currentTimeMillis();
		//		
		LOG.info("Total time=" + (t2 - t1) + " ms");
		if (sCacheType == CacheType.KODA || sCacheType == CacheType.INFINISPAN_KODA)  {
			//OffHeapCache cache = sCacheType == CacheType.KODA? mCache: getISPNOffHeapCache();
			EvictionAlgo algo = mCache.getEvictionAlgo();
			LOG.info("Eviction stats:");
			LOG
					.info("  number of attempts ="
							+ algo.getTotalEvictionAttempts());
			LOG.info("  number of evicted  =" + algo.getTotalEvictedItems());
		} 

		LOG.info("Estimated RPS="
				+ ((double) (sPuts.get() + sGets.get()) * 1000) / (t2 - t1));
		System.exit(0);

	}

	/**
	 * Inits the cache.
	 * 
	 * @throws Exception
	 *             the exception
	 */
	private static void initCache() throws Exception {
		switch (sCacheType) {
		case KODA:
			initCacheKoda();
			break;
		case EHCACHE:
			initCacheEhcache();
			break;
		case EHCACHE_BM:
			initCacheEhcacheBigMemory();
			break;
		case INFINISPAN:
			initCacheInfinispan();
			break;
		case INFINISPAN_KODA:
			initCacheInfinispanKoda();
		case HAZELCAST:
			initCacheHazelcast();
			break;
		case GIGASPACES:
			initCacheGigaspaces();
			break;
		case COHERENCE:
			initCacheCoherence();
			break;
		case XSCALE:
			initCacheXScale();
			break;

		}

	}

	/**
	 * Inits the cache ehcache big memory.
	 */
	@SuppressWarnings("deprecation")
  private static void initCacheEhcacheBigMemory() {
		// Create a CacheManager using defaults
		sEhcacheManager = CacheManager.create();

		// Create a Cache specifying its configuration.

		sEhcacheCache = new Cache(new CacheConfiguration("test", 10000)
				.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LRU)
				.overflowToDisk(false).eternal(false).timeToLiveSeconds(6000)
				.timeToIdleSeconds(6000).diskPersistent(false)
				.diskExpiryThreadIntervalSeconds(0).overflowToOffHeap(true)
				.maxMemoryOffHeap(sBigMemorySize));
		sEhcacheManager.addCache(sEhcacheCache);

	}

	/**
	 * Inits the cache x scale.
	 */
	private static void initCacheXScale() {
		// TODO Auto-generated method stub

	}

	/**
	 * Inits the cache coherence.
	 */
	private static void initCacheCoherence() {
		// TODO Auto-generated method stub

	}

	/**
	 * Inits the cache gigaspaces.
	 */
	private static void initCacheGigaspaces() {
		// TODO Auto-generated method stub

	}

	/**
	 * Inits the cache hazelcast.
	 */
	private static void initCacheHazelcast() {
		// TODO Auto-generated method stub

	}

	/**
	 * Inits the cache infinispan.
	 */
	private static void initCacheInfinispan() {
		EmbeddedCacheManager manager = new DefaultCacheManager();
		Configuration config = new Configuration().fluent()
		  .eviction()
		    .maxEntries((int)sCacheItemsLimit).strategy(EvictionStrategy.LRU)
		    /*.wakeUpInterval(5000L)*/ // TODO - fix it 
		  .expiration()
		    .maxIdle(120000L)
		    .build();

		manager.defineConfiguration("name", config);
		ispnCache = manager.getCache("name");


	}

	/**
	 * Inits the cache infinispan.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws KodaException the koda exception
	 */
//	private static void initCacheInfinispanKoda() {
//		LOG.info("Initializing ISPN - Koda ...");
//		EmbeddedCacheManager manager = new DefaultCacheManager();
//		
//		KodaCacheStoreConfig cfg = new KodaCacheStoreConfig();
//		cfg.setBucketNumber(N);
//		cfg.setCacheNamespace("ns");
//		cfg.setEvictionPolicy("LRU");
//		cfg.setMaxMemory(sMemoryLimit);
//		cfg.setDefaultExpireTimeout(6000);
//		cfg.setKeyClass(ByteArrayWritable.class);
//		cfg.setValueClass(ByteArrayWritable.class);
//		
//		Configuration config = new Configuration().fluent()
//		  .eviction()
//		    .maxEntries(20).strategy(EvictionStrategy.LRU)
//		    .wakeUpInterval(5000L)
//		  .expiration()
//		    .maxIdle(120000L)
//		    .loaders()
//		    	.shared(false).passivation(false).preload(false)
//		    .addCacheLoader(
//		    		cfg
//		    )/*.locking().useLockStriping(true).concurrencyLevel(256)*/.build();
//
//		manager.defineConfiguration("name", config);
//		ispnCacheKoda  = manager.getCache("name");
//		List<CommandInterceptor> chain = ispnCacheKoda.getAdvancedCache().getInterceptorChain();
//		//ispnCacheKoda.getAdvancedCache().addInterceptor( new KodaInterceptor(), chain.size()-1);
//		mCache = getISPNOffHeapCache();
//
//	}

	private static void initCacheInfinispanKoda() throws NativeMemoryException, KodaException {
		LOG.info("Initializing ISPN - Koda ...");
		initCacheKoda();
		OffHeapDataContainer dc = new OffHeapDataContainer(mCache);		
		EmbeddedCacheManager manager = new DefaultCacheManager();						
		Configuration config = new Configuration().fluent().
									dataContainer().
										dataContainer(dc).			    
											build();

		manager.defineConfiguration("name", config);
		ispnCacheKoda  = manager.getCache("name");

//		org.infinispan.Cache<byte[], byte[]> cache = ispnCacheKoda;
		
LOG.info("Done");
	}
	
	
	/**
	 * Inits the cache ehcache.
	 */
	@SuppressWarnings("deprecation")
  private static void initCacheEhcache() {
		// Create a CacheManager using defaults
		sEhcacheManager = CacheManager.create();

		// Create a Cache specifying its configuration.

		sEhcacheCache = new Cache(new CacheConfiguration("test",
				(int) sCacheItemsLimit).memoryStoreEvictionPolicy(
				MemoryStoreEvictionPolicy.LRU).overflowToDisk(false).eternal(
				false).timeToLiveSeconds(6000).timeToIdleSeconds(6000)
				.diskPersistent(false).diskExpiryThreadIntervalSeconds(0));
		sEhcacheManager.addCache(sEhcacheCache);

	}

	/**
	 * Inits the cache kode.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws KodaException the koda exception
	 */
	private static void initCacheKoda() throws NativeMemoryException, KodaException {
		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
		config.setBucketNumber(N);
		config.setMaxMemory(sMemoryLimit);
		config.setEvictionPolicy(sEvictionAlgo);	
		//config.setLRU2QInsertPoint(sLru2qip);
		//config.setDefaultExpireTimeout(6000);
		config.setCacheName("test");
		config.setSerDeBufferSize(100000);
		//config.setMallocConcurrent(true);
		//config.setCompressionEnabled(true);
		//config.setCodecType(CodecType.LZ4);
//		if(sEvictionAlgo.equals("LRU")){
//		  config.setHistogramEnabled(true);
//		}
//		config.setHistogramUpdateInterval(500);
    config.setCandidateListSize(30);
//    config.setOptimizerEnabled(true);
//    config.setOptimizerThreads(2);
//    config.setOptimizerLevel(1);
		//config.setKeyClassName(byte[].class.getName());
		//config.setValueClassName(byte[].class.getName());
		mCache = manager.createCache(config);
		LOG.info("Eviction Algo : "+mCache.getEvictionAlgo().getClass().getName()+" lru2qip="+ sLru2qip);
		LOG.info("Compression="+ mCache.getCompressionCodec());


	}

	/**
	 * Parses the args.
	 * 
	 * @param args
	 *            the args
	 */
	private static void parseArgs(String[] args) {

		int i = 0;
		while (i < args.length) {

			if (args[i].equals(THREADS)) {
				sClientThreads = Integer.parseInt(args[++i]);
			} else if (args[i].equals(BUCKETS)) {
				N = Integer.parseInt(args[++i]);
			} else if (args[i].equals(MAXMEMORY)) {
				sMemoryLimit = Long.parseLong(args[++i]);
			} else if (args[i].equals(MAXITEMS)) {
				sCacheItemsLimit = Long.parseLong(args[++i]);
			} else if (args[i].equals(WRITE_RATIO)) {
				sWriteRatio = Float.parseFloat(args[++i]);
			} else if (args[i].equals(CACHE)) {
				sCacheType = getType(args[++i]);
			} else if (args[i].equals(DURATION)) {
				sTestTime = Long.parseLong(args[++i]) * 1000;
			} else if (args[i].equals(BIGMEMORY_SIZE)) {
				sBigMemorySize = args[++i];
			} else if (args[i].equals(EVICTION)) {
        sEvictionAlgo = args[++i].toUpperCase();
      } else if (args[i].equals(LRU2Q_INSERT_POINT)) {
        sLru2qip = Float.parseFloat(args[++i]);
      }

			i++;
		}

	}

	/**
	 * Gets the type.
	 * 
	 * @param s
	 *            the s
	 * @return the type
	 */
	private static CacheType getType(String s) {
		if (s.equals(KODE))
			return CacheType.KODA;
		if (s.equals(EHCACHE))
			return CacheType.EHCACHE;
		if (s.equals(INFINISPAN))
			return CacheType.INFINISPAN;
		if (s.equals(HAZELCAST))
			return CacheType.HAZELCAST;
		if (s.equals(COHERENCE))
			return CacheType.COHERENCE;
		if (s.equals(XSCALE))
			return CacheType.XSCALE;
		if (s.equals(GIGASPACES))
			return CacheType.GIGASPACES;
		if (s.equals(EHCACHE_BM))
			return CacheType.EHCACHE_BM;
		if(s.equals(INFINISPAN_KODA)){
			return CacheType.INFINISPAN_KODA;
		}
		return null;
	}

	/**
	 * Wait to finish.
	 * 
	 * @param threads
	 *            the threads
	 */
	static void waitToFinish(Thread[] threads) {
		for (int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (Exception e) {
				// ignore
			}
		}
	}

	/**
	 * Start test.
	 * 
	 * @param keyPrefix
	 *            the key prefix
	 * @param number
	 *            the number
	 * @param opNumber
	 *            the op number
	 * @return the execute thread[]
	 */
	static ExecuteThread[] startTest(String[] keyPrefix, int number,
			int opNumber) {
		ExecuteThread[] threadArray = new ExecuteThread[number];
		for (int i = 0; i < number; i++) {
			threadArray[i] = new ExecuteThread(keyPrefix[i], opNumber);
			threadArray[i].start();
		}

		return threadArray;
	}

	/**
	 * The Class ExecuteThread.
	 */
	static class ExecuteThread extends Thread {

		/** The total number of queris. */
		int n;

		/** The m thread index. */
		int mThreadIndex;

		/** The m total threads. */
		int mTotalThreads;

		/** Statistics section. */
		private double avgTime;

		/** The max time. */
		private double maxTime;

		/** The median time. */
		private double medianTime;

		/** The time99. */
		private double time99;

		/** The time999. */
		private double time999;

		/** The time9999. */
		private double time9999;
		
		/** */
		//private double time99999;

		/** The total time. */
		private long totalTime; // in nanoseconds

		/** The total requests. */
		private long totalRequests;

		/** The Constant NN. */
		final private static int NN = 1000;

		/** The Constant MM. */
		final private static int MM = 20;

		/** The request times. */
		private long[] requestTimes = new long[NN + MM];

		/** The copy array. */
		private long[] copyArray = new long[NN + MM];

		/** The MI n_ time. */
		final long MIN_TIME = 5000; // 5 microsec

		/** The counter. */
		private int counter;

		/** The icounter. */
		private int icounter;

		/** The tt. */
		private long tt;

		/** The stat time. */
		private long statTime;

		/** The start time. */
		private long startTime = System.nanoTime();
		
		/** The max item number. */
		long maxItemNumber = 0;

		/** The r. */
		Random r;// = new Random();

		/** The buf. */
		ByteBuffer buf;

		/** The buf ptr. */
		long bufPtr;

		/** The key buf. */
		byte[] keyBuf;

		/** The m prefix. */
		String mPrefix;

		/** The values. */
		byte[][] values;

		/** The value holder. */
		byte[] valueHolder;
		
		/** The m inner monkey. */
		byte[] mInnerMonkey;

		/** The m last monkey time. */
		long mLastMonkeyTime;

		/** The INTE r_ monkey. */
		long INTER_MONKEY = 100;// 5 msecs

		/** The is read request. */
		boolean[] isReadRequest = new boolean[1011];

		/** The m read request index. */
		int mReadRequestIndex;

		/** The m get offsets. */
		float[] mGetOffsets = new float[1011]; // random floats between 0 and 1

		/** The m get offsets index. */
		int mGetOffsetsIndex;

		/**
		 * Inits the random replacement.
		 */
		private void initRandomReplacement() {
			for (int i = 0; i < isReadRequest.length; i++) {
				float f = r.nextFloat();
				if (f > sWriteRatio) {
					isReadRequest[i] = true;
				} else {
					isReadRequest[i] = false;
				}
				mGetOffsets[i] = f;
			}

		}

		/**
		 * Checks if is read request.
		 * 
		 * @return true, if is read request
		 */
		private final boolean isReadRequest() {
			boolean v = isReadRequest[mReadRequestIndex++];
			if (mReadRequestIndex == isReadRequest.length) {
				mReadRequestIndex = 0;
			}
			return v;
		}

		/**
		 * Gets the next get offset.
		 * 
		 * @param max
		 *            the max
		 * @return the next get offset
		 */

        private final long getNextGetOffset(long max) {
			// LOG.info("max="+max);
			float f = mGetOffsets[mGetOffsetsIndex++];
			if (mGetOffsetsIndex == mGetOffsets.length) {
				mGetOffsetsIndex = 0;
			}

			return (long) (f * max);
		}

		/**
		 * Calculate stats.
		 */
		private void calculateStats() {
			// avgTime

			double sum = 0.d;
			double max = Double.MIN_VALUE;
			for (int i = 0; i < requestTimes.length; i++) {
				sum += ((double) requestTimes[i]) / 1000;
			}
			// avgTime
			avgTime = (avgTime * (totalRequests - requestTimes.length) + sum)
					/ totalRequests;

			// sort
			Arrays.sort(requestTimes);
						
			
			max = ((double) requestTimes[requestTimes.length - 1]) / 1000;
			// maxTime
			if (max > maxTime)
				maxTime = max;
			double median = ((double) (requestTimes[requestTimes.length
					- (counter) / 2])) / 1000;// microsecs

			if (medianTime == 0.d) {
				medianTime = median;
			} else {
				medianTime = (medianTime * (totalRequests - (counter)) + median
						* (counter))
						/ totalRequests;
			}

			double t99 = ((double) requestTimes[requestTimes.length - 1000]) / 1000;
			if (time99 == 0.d) {
				time99 = t99;
			} else {
				time99 = (time99 * (totalRequests - (counter)) + t99
						* (counter))
						/ totalRequests;
			}
			double t999 = ((double) requestTimes[requestTimes.length - 100]) / 1000;
			if (time999 == 0.d) {
				time999 = t999;
			} else {
				time999 = (time999 * (totalRequests - (counter)) + t999
						* (counter))
						/ totalRequests;
			}

			double t9999 = ((double) requestTimes[requestTimes.length - 10]) / 1000;
			if (time9999 == 0.d) {
				time9999 = t9999;
			} else {
				time9999 = (time9999 * (totalRequests - counter) + t9999
						* counter)
						/ totalRequests;
			}
			
//			double t99999 = ((double) requestTimes[requestTimes.length - 10]) / 1000;
			
			//System.out.println(t99999);
//			if (time99999 == 0.d) {
//				time99999 = t99999;
//			} else {
//				time99999 = (time99999 * (totalRequests - counter) + t99999
//						* counter)
//						/ totalRequests;
//			}
			//System.out.println(time99999);
			counter = 0;
			System
					.arraycopy(copyArray, 0, requestTimes, 0,
							requestTimes.length);
		}

		public int getCounter()
		{
			return counter;
		}
		
		/**
		 * in microsecnds.
		 * 
		 * @return the avg time
		 */
		public double getAvgTime() {
			return avgTime;
		}

		/**
		 * in microseconds.
		 * 
		 * @return the max time
		 */
		public double getMaxTime() {
			return maxTime;
		}

		/**
		 * Gets the requests per sec.
		 * 
		 * @return the requests per sec
		 */
		public double getRequestsPerSec() {
			if (totalTime > 0) {
				double secs = ((double) totalTime) / 1000000000;
				return totalRequests / secs;
			} else {
				return 0;
			}
		}

		/**
		 * Gets the total requests.
		 * 
		 * @return the total requests
		 */
		public long getTotalRequests() {
			long v = totalRequests;
			//totalRequests = 0;
			return v;
		}

		/**
		 * 50% of requests have latency < medianTime. In microseconds
		 * 
		 * @return the median time
		 */
		public double getMedianTime() {
			return medianTime;
		}

		/**
		 * 99% of requests have latency < time99. In microseconds
		 * 
		 * @return the time99
		 */
		public double getTime99() {
			return time99;
		}

		/**
		 * 99.9% of requests have latency < time999. In microseconds
		 * 
		 * @return the time999
		 */
		public double getTime999() {
			return time999;
		}

		/**
		 * 99.99% of requests have latency < time9999. In microseconds
		 * 
		 * @return the time9999
		 */
		public double getTime9999() {
			return time9999;
		}

//		public double getTime99999() {
//			return time99999;
//		}
		
		/**
		 * Instantiates a new execute thread.
		 * 
		 * @param keyPrefix
		 *            the key prefix
		 * @param n
		 *            the n
		 */
		public ExecuteThread(String keyPrefix, int n) {
			super(keyPrefix);
			this.n = n;
			this.mPrefix = keyPrefix;
			r = new Random(Utils.hashString(keyPrefix, 0));
			initRandomReplacement();

		}

		/**
		 * Instantiates a new execute thread.
		 * 
		 * @param keyPrefix
		 *            the key prefix
		 * @param index
		 *            the index
		 * @param total
		 *            the total
		 */
		public ExecuteThread(String keyPrefix, int index, int total) {
			super(keyPrefix);
			mThreadIndex = index;
			mTotalThreads = total;

		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		public void run() {
			try {

				switch (sCacheType) {
				case KODA:
					testPerfKode(getName());
					break;
				case EHCACHE:
				case EHCACHE_BM:
					testPerfEhcache(getName());
					break;
				case INFINISPAN:
				case INFINISPAN_KODA:
					testPerfInfinispan(getName());
					break;
				}
			} catch (Exception e) {
				LOG.error(e);
			}
		}

		/**
		 * Test perf ehcache.
		 * 
		 * @param key
		 *            the key
		 */
		private void testPerfEhcache(String key) {
			if (sCacheType == CacheType.EHCACHE) {
				LOG.info("Ehcache Performance test : "
						+ Thread.currentThread().getName());
			} else {
				LOG.info("Ehcache - BigMemory Performance test : "
						+ Thread.currentThread().getName());
			}

			// There is 100 different values between 200 -800
			// Creates values;
			values = new byte[100][];
			for (int i = 0; i < 100; i++) {
				values[i] = new byte[r.nextInt(600) + 200];
			}

			byte[] keySuff = key.getBytes();
			keyBuf = new byte[keySuff.length + 4];
			System.arraycopy(keySuff, 0, keyBuf, 4, keySuff.length);

			// TODO Avoid JIT compilation effect

			try {
				int c = 0;
				// JIT warm up
				while (c++ < 1000) {
					innerLoopEhcache();
				}

				totalTime = 0;
				totalRequests = 0;
				tt = System.nanoTime();
				icounter = 0;
				counter = 0;
				statTime = 0;
				long t1 = System.currentTimeMillis();
				long stopTime = t1 + sTestTime;

				while (System.currentTimeMillis() < stopTime) {
					innerLoopEhcache();
				}
				LOG.info("Ehcache: " + getName() + ": Finished.");
			} catch (Exception e) {

				LOG.error(e);
				System.exit(-1);
			}

		}

		/**
		 * Monkey call emulates external stream of memory allocation requests.
		 * 
		 * @param tt1
		 *            the tt1
		 */
		private void monkeyCall(long tt1) {
		/*	if (tt1 - mLastMonkeyTime > INTER_MONKEY * 1000000) {
				mInnerMonkey = new byte[1024 + r.nextInt(4096)];
				mLastMonkeyTime = tt1;
			}
			*/
		}

		/**
		 * Inner loop ehcache.
		 */
		private final void innerLoopEhcache() {
			long tt1 = System.nanoTime();

			monkeyCall(tt1);

			// float f = r.nextFloat();
			boolean isReadRequest = isReadRequest();
			if (isReadRequest) {
				try {
					// long l = maxItemNumber > 0 ? (Math.abs(r.nextLong()) %
					// maxItemNumber): 0;
					long l = getNextGetIndex();
					// byte[] bkey = getKeyLong(l, keyBuf);
					String skey = getKeyLongCopy(l, mPrefix);
					// Element el = sEhcacheCache.get(new String(bkey));
					Element el = sEhcacheCache.get(skey);
					if(el != null) sInCache.incrementAndGet();
					sGets.incrementAndGet();
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error(e);
					System.exit(-1);
				}
			} else {
				try {
					int i = r.nextInt(100);
					// byte[] bkey = getKeyLongCopy(maxItemNumber++, keyBuf);
					// sEhcacheCache.put(new Element(new String( bkey),
					// getValueCopy(values[i])));
					String skey = getKeyLongCopy(maxItemNumber++, mPrefix);
					sEhcacheCache
							.put(new Element(skey, getValueCopy(values[i])));
					sPuts.incrementAndGet();

				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("put call.", e);
					System.exit(-1);
				}
			}
			icounter++;
			totalRequests++;
			long tt2 = System.nanoTime();
			if (tt2 - tt1 > MIN_TIME) {
				// process all previous
				long lastt = tt2 - tt1;
				long rt = (icounter > 1) ? (tt1 - tt - statTime)
						/ (icounter - 1) : 0;
				for (int i = 0; i < icounter - 1; i++) {
					requestTimes[counter++] = rt;
				}
				requestTimes[counter++] = lastt;
				totalTime += (tt2 - tt) - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else if (tt2 - tt > MIN_TIME) {
				long rt = (tt2 - tt - statTime) / icounter;
				for (int i = 0; i < icounter; i++) {
					requestTimes[counter++] = rt;
				}
				totalTime += tt2 - tt - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else {
				// continue

			}

			if (counter >= NN/* requestTimes.length */) {
				long ttt1 = System.nanoTime();
				calculateStats();
				long ttt2 = System.nanoTime();
				statTime = ttt2 - ttt1;

			}
		}

		/**
		 * Test perf Koda.
		 * 
		 * @param key
		 *            the key
		 * @throws NativeMemoryException
		 *             the j emalloc exception
		 */
		private void testPerfKode(String key) throws NativeMemoryException {
			LOG.info("Koda Performance test. Cache size =" + mCache.size()
					+ ": " + Thread.currentThread().getName());

			// TODO init Kode cache

			buf = NativeMemory.allocateDirectBuffer(256, 100000);
			bufPtr = NativeMemory.getBufferAddress(buf);
      Random r = new Random();
      
			// There is 100 different values between 200 -800
			// Creates values;
			values = new byte[1000][];
			for (int i = 0; i < 1000; i++) {
				values[i] = new byte[r.nextInt(10000) + 60000];
				//values[i] = new byte[4];
				r.nextBytes(values[i]);
				for(int k=0; k < 2*values[i].length/3; k++){
				  values[i][k] = (byte)5;
				}
			}

			valueHolder = new byte[200000];
			
			byte[] keySuff = key.getBytes();
			keyBuf = new byte[keySuff.length + 4];
			System.arraycopy(keySuff, 0, keyBuf, 4, keySuff.length);

			//keyBuf = new byte[12];
			//byte[] keySuff = key.getBytes();
			//System.arraycopy(keySuff, keySuff.length >=12 ? keySuff.length -12: 0, 
			//		keyBuf, 0,  keySuff.length >=12 ? 12: keySuff.length);
			// TODO Avoid JIT compilation effect

			try {
				int c = 0;
				// JIT warm up
				while (c++ < 1000) {
					innerLoopKode();
				}

				totalTime = 0;
				totalRequests = 0;
				tt = System.nanoTime();
				icounter = 0;
				counter = 0;
				statTime = 0;
				long t1 = System.currentTimeMillis();
				long stopTime = t1 + sTestTime;
				while (System.currentTimeMillis() < stopTime) {
					innerLoopKode();
				}
				checkCacheStats();
				LOG.info(getName() + ": Finished.");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error(e);
				System.exit(-1);
			}

		}

		
		private void checkCacheStats() throws NativeMemoryException, IOException {
      LOG.info("Max ID="+maxItemNumber+" cache size="+mCache.size());
      int totalFound = 0;
      for(long n = maxItemNumber - mCache.size() / sClientThreads; n < maxItemNumber; n++ ){
        byte[] bkey = getKeyLong(n, keyBuf);
        Object value = mCache.get(bkey);
        if(value != null) totalFound++;
      }
      
      LOG.info("Cache Size ="+mCache.size()+" found="+totalFound+" R="+((double)totalFound)/mCache.size());
    }

    /**
		 * Test perf Koda.
		 * 
		 * @param key
		 *            the key
		 * @throws NativeMemoryException
		 *             the j emalloc exception
		 */
		private void testPerfInfinispan(String key) throws NativeMemoryException {
			if (sCacheType == CacheType.INFINISPAN) {
				LOG.info("Infinispan Performance test : "
						+ Thread.currentThread().getName());
			} else {
				LOG.info("Infinispan - Koda Performance test : "
						+ Thread.currentThread().getName());
			}

			// TODO init Kode cache

			//buf = NativeMemory.allocateDirect(256, 1000);
			//buf.order(ByteOrder.nativeOrder());
			//bufPtr = NativeMemory.getBufferAddress(buf);

			// There is 100 different values between 200 -800
			// Creates values;
			values = new byte[100][];
			for (int i = 0; i < 100; i++) {
				values[i] = new byte[r.nextInt(600) + 200];
			}

			byte[] keySuff = key.getBytes();
			keyBuf = new byte[keySuff.length + 4];
			System.arraycopy(keySuff, 0, keyBuf, 4, keySuff.length);

			// TODO Avoid JIT compilation effect

			try {
				int c = 0;
				// JIT warm up
				if(sCacheType == CacheType.INFINISPAN){
					while (c++ < 1000) {
						innerLoopInfinispan();
					}
				} else{
					while (c++ < 1000) {
						innerLoopInfinispanKoda();
					}
				}

				totalTime = 0;
				totalRequests = 0;
				tt = System.nanoTime();
				icounter = 0;
				counter = 0;
				statTime = 0;
				long t1 = System.currentTimeMillis();
				long stopTime = t1 + sTestTime;

				if(sCacheType == CacheType.INFINISPAN){
					while (System.currentTimeMillis() < stopTime) {
						innerLoopInfinispan();
					}
				} else{
					while (System.currentTimeMillis() < stopTime) {
						innerLoopInfinispanKoda();
					}
				}
				LOG.info(getName() + ": Finished.");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error(e);
				System.exit(-1);
			}

		}
		
		/**
		 * Inner loop infinispan.
		 */
		private void innerLoopInfinispan() {
			long tt1 = System.nanoTime();
			monkeyCall(tt1);
			byte[] key =  null; //new ByteArrayWritable();
			byte[] value = null;//new ByteArrayWritable();
			boolean isReadRequest = isReadRequest();// f > sWriteRatio;
			if (isReadRequest) {
				try {
					// long l = maxItemNumber > 0 ? (Math.abs(r.nextLong()) %
					// maxItemNumber): 0;

					long l = getNextGetIndex();

					key = getKeyLongCopy(l, keyBuf);
					value = ispnCache.get(key);
					//mCache.getNative(bkey, buf, bufPtr);
					
					// sGets.incrementAndGet();
					// buf.position(0);
					// if(buf.getInt() != 0) sInCache.incrementAndGet();

				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("get native call.", e);
					System.exit(-1);
				}
			} else {
				try {
					int i = r.nextInt(100);
					key = getKeyLongCopy(maxItemNumber++, keyBuf);
					// sPuts.incrementAndGet();
					value = getValueCopy(values[i]);
					ispnCache.put(key, value);
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("put call.", e);
					System.exit(-1);
				}
			}
			icounter++;
			totalRequests++;
			long tt2 = System.nanoTime();
			if (tt2 - tt1 > MIN_TIME) {
				// process all previous
				long lastt = tt2 - tt1;
				long rt = (icounter > 1) ? (tt1 - tt - statTime)
						/ (icounter - 1) : 0;
				for (int i = 0; i < icounter - 1; i++) {
					requestTimes[counter++] = rt;
				}
				requestTimes[counter++] = lastt;
				totalTime += (tt2 - tt) - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else if (tt2 - tt > MIN_TIME) {
				long rt = (tt2 - tt - statTime) / icounter;
				for (int i = 0; i < icounter; i++) {
					requestTimes[counter++] = rt;
				}
				totalTime += tt2 - tt - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else {
				// continue

			}

			if (counter >= NN/* requestTimes.length */) {
				long ttt1 = System.nanoTime();
				calculateStats();
				long ttt2 = System.nanoTime();
				statTime = ttt2 - ttt1;

			}
		}

		/**
		 * Inner loop infinispan koda.
		 */
		private void innerLoopInfinispanKoda() {
			long tt1 = System.nanoTime();
			monkeyCall(tt1);
			byte[] key = null;
			byte[] value = null;
			boolean isReadRequest = isReadRequest();// f > sWriteRatio;
			if (isReadRequest) {
				try {

					long l = getNextGetIndex();
					key = getKeyLong(l, keyBuf);
					value = ispnCacheKoda.get(key);					
					sGets.incrementAndGet();
					if(value != null) {
						sInCache.incrementAndGet();
					}


				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("get native call.", e);
					System.exit(-1);
				}
			} else {
				try {
					int i = r.nextInt(100);
					key = getKeyLongCopy(maxItemNumber++, keyBuf);
					sPuts.incrementAndGet();
					value = getValueCopy(values[i]);
					ispnCacheKoda.getAdvancedCache().
					withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_LOAD).put(key, value);
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("put call.", e);
					System.exit(-1);
				}
			}
			icounter++;
			totalRequests++;
			long tt2 = System.nanoTime();
			if (tt2 - tt1 > MIN_TIME) {
				// process all previous
				long lastt = tt2 - tt1;
				long rt = (icounter > 1) ? (tt1 - tt - statTime)
						/ (icounter - 1) : 0;
				for (int i = 0; i < icounter - 1; i++) {
					requestTimes[counter++] = rt;
				}
				requestTimes[counter++] = lastt;
				totalTime += (tt2 - tt) - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else if (tt2 - tt > MIN_TIME) {
				long rt = (tt2 - tt - statTime) / icounter;
				for (int i = 0; i < icounter; i++) {
					requestTimes[counter++] = rt;
				}
				totalTime += tt2 - tt - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else {
				// continue

			}

			if (counter >= NN/* requestTimes.length */) {
				long ttt1 = System.nanoTime();
				calculateStats();
				long ttt2 = System.nanoTime();
				statTime = ttt2 - ttt1;

			}
			
		}

		/**
		 * Next int.
		 * 
		 * @param max
		 *            the max
		 * @return the int
		 */
		private final int nextInt(final int max) {
			float f = mGetOffsets[mGetOffsetsIndex++];
			if (mGetOffsetsIndex == mGetOffsets.length) {
				mGetOffsetsIndex = 0;
			}
			return (int) (f * max);
		}

		/**
		 * Unsafe if cacheSize >.
		 * 
		 * @return the next get index
		 */
		private final long getNextGetIndex() {

			long cacheSize = (sCacheItemsLimit > 0) ? sCacheItemsLimit : mCache
					.size();
			if (maxItemNumber > cacheSize) {
				// return maxItemNumber -
				// getNextGetOffset(cacheSize);//(Math.abs(r.nextLong()) %
				// cacheSize);
				return maxItemNumber - ((nextInt((int) cacheSize / sClientThreads)));
			} else {
				// return maxItemNumber > 0?getNextGetOffset(maxItemNumber):0;//
				// Math.abs(r.nextLong()) % maxItemNumber: 0;
				// return maxItemNumber > 0?Math.abs(r.nextLong()) %
				// maxItemNumber: 0;
				return maxItemNumber > 0 ? Math
						.abs(nextInt((int) maxItemNumber)) : 0;
			}
		}

		/**
		 * Inner loop.
		 * 
		 * @throws NativeMemoryException
		 *             the j emalloc exception
		 */
		private final void innerLoopKode() throws NativeMemoryException {

			long tt1 = System.nanoTime();
			//monkeyCall(tt1);

			boolean isReadRequest = isReadRequest();// f > sWriteRatio;
			if (isReadRequest) {
				try {
					long l = getNextGetIndex();

					byte[] bkey = getKeyLong(l, keyBuf);
					mCache.get(bkey, valueHolder);

				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("get native call.", e);
					System.exit(-1);
				}
			} else {
				try {
					int i = r.nextInt(1000);
					byte[] bkey = getKeyLong(maxItemNumber++, keyBuf);//getKeyLongCopy(maxItemNumber++, keyBuf);

					byte[] value = values[i];//getValueCopy(values[i]);
					mCache.put(bkey, value);

				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("put call.", e);
					System.exit(-1);
				}
			}
			icounter++;
			totalRequests++;
			long tt2 = System.nanoTime();
			boolean enableTiming = (tt2 - startTime) > (100000000000L); 
			if (tt2 - tt1 > MIN_TIME) {
				// process all previous
				long lastt = tt2 - tt1;
				
				long rt = (icounter > 1) ? (tt1 - tt - statTime)
						/ (icounter - 1) : 0;
				//if(enableTiming){
					for (int i = 0; i < icounter - 1; i++) {
						requestTimes[counter++] = rt;
					}
					requestTimes[counter++] = lastt;
				//}
				totalTime += (tt2 - tt) - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else if (tt2 - tt > MIN_TIME) {
				long rt = (tt2 - tt - statTime) / icounter;
				
				//if(enableTiming){
					for (int i = 0; i < icounter; i++) {
						requestTimes[counter++] = rt;
					}
				//}
				
				totalTime += tt2 - tt - statTime;
				tt = tt2;
				icounter = 0;
				statTime = 0;
			} else {
				// continue

			}

			if (counter >= NN/* requestTimes.length */) {
				long ttt1 = System.nanoTime();
				calculateStats();
				long ttt2 = System.nanoTime();
				statTime = ttt2 - ttt1;

			}
		}
	
	
	
	}

	static long startTime = System.currentTimeMillis();
	
	/**
	 * The Class StatsCollector.
	 */
	static class StatsCollector extends Thread {

		/** The m threads. */
		ExecuteThread[] mThreads;

		/** The m interval. */
		long mInterval;
		
		/** The m start time. */
		long mStartTime;
		/**
		 * Instantiates a new stats collector.
		 * 
		 * @param interval
		 *            the interval
		 * @param sources
		 *            the sources
		 */
		public StatsCollector(long interval, ExecuteThread[] sources) {
			super("StatsCollector");
			setDaemon(true);
			this.mThreads = sources;
			this.mInterval = interval;
			this.mStartTime = System.currentTimeMillis();
		
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Thread#run()
		 */
		public void run() {
			while (true) {
				try {
					Thread.sleep(mInterval);
				} catch (Exception e) {

				}
				double rps = 0.d;
				double max = 0.d;
				double avg = 0.d;
				double median = 0.d;
				double t99 = 0.d;
				double t999 = 0.d;
				double t9999 = 0.d;
				//double t99999 = 0.d;
				long totalRequests =0;
				for (ExecuteThread et : mThreads) {
					rps += et.getRequestsPerSec();
					totalRequests += et.getTotalRequests();
					double m = et.getMaxTime();
					if (m > max)
						max = m;
					avg += et.getAvgTime();
					median += et.getMedianTime();
					t99 += et.getTime99();
					t999 += et.getTime999();
					t9999 += et.getTime9999();
					//t99999 += et.getTime99999();
				}

				avg /= mThreads.length;
				median /= mThreads.length;
				t99 /= mThreads.length;
				t999 /= mThreads.length;
				t9999 /= mThreads.length;
				// rps = ((double)totalRequests*1000)/ mInterval;

                float hitRatio = (sGets.get() != 0)?
				((float)sInCache.get())/sGets.get():0;
				rps = totalRequests*1000/(System.currentTimeMillis() - mStartTime);
				LOG.info("\n\nRPS=" + rps + "\n" + "MAX=" + max + "\nAVG="
						+ avg + "\nMEDIAN=" + median + "\n99%=" + t99
						+ "\n99.9%=" + t999 + "\n99.99%=" + t9999 + "\nSIZE="
						+ getMemAllocated() + "\nNATIVE SIZE="+NativeMemory.getMalloc().memoryAllocated()+"\nITEMS=" + getTotalItems()+"\nGETS="+getTotalRequests()+
						" HITS="+getTotalHits()+
						"\nRAW SIZE="+getRawSize()+" COMP_RATIO="+getAvgCompRatio()+"\nEVICTED="+getEvictedCount()+
						"\nEVICTION ATTEMPTS="+ getTotalEvictionAttempts()+"\nEVICTION FAILED="+getTotalFailedEvictionAttempts()+
						"\nEVICTION FATAL="+getTotalFailedFatalEvictionAttempts()+
						"\nFAILED PUTS="+ getTotalFailedPuts()+
						"\nTIME ="+ (System.currentTimeMillis() - startTime)/1000 +" sec");
//				  LOG.info("TIME ="+ (System.currentTimeMillis() - startTime)/1000 +" sec");
//				NumericHistogram hist = mCache.getObjectHistogram();
//				LOG.info("\n"+hist.toString(10));
//				LOG.info("Object Max Age ~"+ (long)(hist.quantile(1)- hist.quantile(0))+"ms");


			}
		}
	}

	/**
	 * Gets the total items.
	 * 
	 * @return the total items
	 */
	@SuppressWarnings("deprecation")
  public static String getTotalItems() {
		switch (sCacheType) {
		case KODA:
			return mCache.size() + "";
		case EHCACHE:
			return sEhcacheCache.getMemoryStoreSize() + "";
		case EHCACHE_BM:
			return sEhcacheCache.getOffHeapStoreSize() + "";
		case INFINISPAN:
			return ispnCache.size() + "";
		case INFINISPAN_KODA:
			return mCache.size() + "";
		}
		return "N/A";
	}

	/**
	 * Gets the mem allocated.
	 * 
	 * @return the mem allocated
	 */
	public static String getMemAllocated() {
		switch (sCacheType) {
		case KODA:
			return mCache.getTotalAllocatedMemorySize() + "";
		case INFINISPAN_KODA:
			return mCache.getTotalAllocatedMemorySize() + "";

		}
		return "N/A";
	}

	/**
	 * Gets the raw size.
	 *
	 * @return the raw size
	 */
	public static String getRawSize()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getRawDataSize()+"";
		}
		return "N/A";
	}
	
	/**
	 * Gets the compressed size.
	 *
	 * @return the compressed size
	 */
	public static String getCompressedSize()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getCompressedDataSize()+"";
		}
		return "N/A";
	}
	
	/**
	 * Gets the avg comp ratio.
	 *
	 * @return the avg comp ratio
	 */
	public static String getAvgCompRatio()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getAverageCompressionRatio()+"";
		}
		return "N/A";
	}
	
	/**
	 * Gets the evicted count.
	 *
	 * @return the evicted count
	 */
	public static String getEvictedCount()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getEvictedCount()+"";
		}
		return "N/A";
	}
	
	
	public static String getTotalEvictionAttempts()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getTotalEvictionAttempts()+"";
		}
		return "N/A";
	}
	
	public static String getTotalFailedEvictionAttempts()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getFailedEvictionAttempts()+"";
		}
		return "N/A";
	}	
	
	public static String getTotalFailedFatalEvictionAttempts()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getFailedFatalEvictionAttempts()+"";
		}
		return "N/A";
	}
	
	 public static String getTotalFailedPuts()
	  {
	    switch(sCacheType)
	    {
	      case KODA: return mCache.getFailedPuts()+"";
	    }
	    return "N/A";
	  }
	
	/**
	 * Gets the total requests.
	 *
	 * @return the total requests
	 */
	public static String getTotalRequests()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getTotalRequestCount()+"";
		}
		return "N/A";
	}
	
	/**
	 * Gets the total hits.
	 *
	 * @return the total hits
	 */
	public static String getTotalHits()
	{
		switch(sCacheType)
		{
			case KODA: return mCache.getHitCount()+"";
		}
		return "N/A";
	}
	
	/**
	 * Gets the iSPN off heap cache.
	 *
	 * @return the iSPN off heap cache
	 */
//	private static OffHeapCache getISPNOffHeapCache() {
//		List<CacheLoaderConfig> list = ispnCacheKoda.getAdvancedCache()
//				.getConfiguration().getCacheLoaders();
//		KodaCacheStoreConfig cacheCfg = (KodaCacheStoreConfig) list.get(0);
//		KodaCacheStore store = cacheCfg.getCacheStore();
//		OffHeapCache cache = store.getOffHeapCache();
//		return cache;
//	}
}

class IntWritable implements Writable<IntWritable>
{
	
	
	private int value;
	public IntWritable(int v)
	{
		this.value  = v;
	}
	
	
	public int getValue()
	{
		return value;
	}
	
	public void setValue(int v)
	{
		this.value = v;
	}
	
	@Override
	public int getID() {

		return Serializer.START_ID - 5;
	}

	@Override
	public void read(ByteBuffer buf) throws IOException {
		value = buf.getInt();
		
	}


	@Override
	public void write(ByteBuffer buf) throws IOException {
		buf.putInt(value);		
	}

	@Override
	public void read(ByteBuffer buf, IntWritable obj) throws IOException {

		obj.setValue(buf.getInt());
	}
	
}
