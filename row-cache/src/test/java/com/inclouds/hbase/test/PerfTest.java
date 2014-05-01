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
package com.inclouds.hbase.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.inclouds.hbase.rowcache.RConstants;
import com.inclouds.hbase.rowcache.RowCache;
import com.koda.KodaException;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionAlgo;
import com.koda.util.Utils;

// TODO: Auto-generated Javadoc
/**
 * The Class OffHeapHashMapPerfTest.
 */
public class PerfTest {



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


	/** The Constant DURATION. */
	private final static String DURATION = "-duration";


	/** The seq number. */
	private static AtomicLong seqNumber = new AtomicLong(0);

	/** The N. */
	private static int N = 1024 * 1024;

	
	/** Data size. */
	private static int M = 10000;
	
	
	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(PerfTest.class);



	/** The s test time. */
	private static long sTestTime = 600000;// 600 secs

	/** The s write ratio. */
	private static float sWriteRatio = 0.1f; // 10% puts - 90% gets

	/** The s interval. */
	private static long sInterval = 5000;

	/** Memory limit in bytes - used for Koda. */
	private static long sMemoryLimit = 1000000000L; // 1G by default

	/** Cache items limit - used for all other caches. */
	private static long sCacheItemsLimit = 15000000; // 1.5 M by default

	/** Number of client threads. */
	private static int sClientThreads = 1; // by default


	/** The s puts. */
	private static AtomicLong sPuts = new AtomicLong(0);

	/** The s gets. */
	private static AtomicLong sGets = new AtomicLong(0);

	/** The s in cache. */
	private static AtomicLong sInCache = new AtomicLong(0);

	
	/** The s cache. */
	static OffHeapCache sCache;
	
	
	/** The table a. */
	protected static byte[] TABLE_A = "TABLE_A".getBytes();
	
	/** The table b. */
	protected static byte[] TABLE_B = "TABLE_B".getBytes();
	
	/** The table c. */
	protected static byte[] TABLE_C = "TABLE_C".getBytes();
	
	/* Families */
	/** The families. */
	protected static byte[][] FAMILIES = new byte[][]
	      {"fam_a".getBytes(), "fam_b".getBytes(), "fam_c".getBytes()};
	
	
	/* Columns */
	/** The columns. */
	protected  static byte[][] COLUMNS = 
	{"col_a".getBytes(), "col_b".getBytes(),  "col_c".getBytes()};
	
	/** The data. */
	static List<List<KeyValue>> data ;
	
	
	/** The versions. */
	//static int VERSIONS = 10;
	
	/** The cache. */
	static RowCache cache;
	
	/* All CF are cacheable */
	/** The table a. */
	static HTableDescriptor tableA;
	/* fam_c not cacheable */
	/** The table b. */
	static HTableDescriptor tableB;
	
	/* Not row cacheable */
	/** The table c. */
	static HTableDescriptor tableC;
	
	/** The min versions. */
	static int minVersions = 1;
	
	/** The max versions. */
	static int maxVersions = 10;
	
	/** The s row. */
	static byte[] sRow = "row-xxx-xxx-xxx".getBytes();


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
		
		LOG.info("Total time=" + (t2 - t1) + " ms");

		EvictionAlgo algo = sCache.getEvictionAlgo();
		LOG.info("Eviction stats:");
		LOG.info("  number of attempts ="
							+ algo.getTotalEvictionAttempts());
		LOG.info("  number of evicted  =" + algo.getTotalEvictedItems());

		LOG.info("Estimated RPS="
				+ ((double) (sPuts.get() + sGets.get()) * 1000) / (t2 - t1));

	}



	/**
	 * Inits the cache kode.
	 *
	 * @throws NativeMemoryException the j emalloc exception
	 * @throws KodaException the koda exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void initCache() throws NativeMemoryException, KodaException, IOException {

		
		if( data != null) return;
		
		long start = System.currentTimeMillis();
		data = generateData(M);
		LOG.info("Generating "+M+" rows took: "+(System.currentTimeMillis() - start)+" ms");
		LOG.info("Allocated JVM heap: "+( Runtime.getRuntime().maxMemory() - Runtime.getRuntime().freeMemory()));
		Configuration conf = new Configuration();

	    // Cache configuration
	    conf.set(RowCache.ROWCACHE_MAXMEMORY, Long.toString(sMemoryLimit));
	    //conf.set(CacheConfiguration.EVICTION_POLICY, "LRU");
	    conf.set(RowCache.ROWCACHE_MAXITEMS, Long.toString(N));
	    conf.set(RowCache.ROWCACHE_COMPRESSION, "LZ4");

	    cache = new RowCache();
	    cache.start(conf);
	    sCache = cache.getOffHeapCache();
	    createTables();
		
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
			} else if (args[i].equals(DURATION)) {
				sTestTime = Long.parseLong(args[++i]) * 1000;
			} 

			i++;
		}

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
		
		/** The total time. */
		//private double time99999;

		/** The total time. */
		private long totalTime; // in nanoseconds

		/** The total requests. */
		private long totalRequests;

		/** The Constant NN. */
		final private static int NN = 100000;

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
		@SuppressWarnings("unused")
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
		@SuppressWarnings("unused")
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

		/**
		 * Gets the counter.
		 *
		 * @return the counter
		 */
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
				testPerf(getName());
			} catch (Exception e) {
				LOG.error(e);
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
		private void testPerf(String key) throws NativeMemoryException {
			LOG.info("RowCache Performance test. Cache size =" + sCache.size()
					+ ": " + Thread.currentThread().getName());

			// TODO init Kode cache

			buf = NativeMemory.allocateDirectBuffer(256, 1000);
			bufPtr = NativeMemory.getBufferAddress(buf);

			// There is 100 different values between 200 -800
			// Creates values;
			values = new byte[1000][];
			for (int i = 0; i < 1000; i++) {
				values[i] = new byte[r.nextInt(10000) + 200];
				//values[i] = new byte[4];
			}

			valueHolder = new byte[20000];
			
			byte[] keySuff = key.getBytes();
			keyBuf = new byte[keySuff.length + 4];
			System.arraycopy(keySuff, 0, keyBuf, 4, keySuff.length);



			try {
				int c = 0;
				// JIT warm up
				while (c++ < 1000) {
					innerLoop();
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
					innerLoop();
				}
				LOG.info(getName() + ": Finished.");
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error(e);
				System.exit(-1);
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

			long cacheSize = (sCacheItemsLimit > 0) ? sCacheItemsLimit : sCache
					.size();
			long maxItemNumber = seqNumber.get();
			if (maxItemNumber > cacheSize) {
				// return maxItemNumber -
				// getNextGetOffset(cacheSize);//(Math.abs(r.nextLong()) %
				// cacheSize);
				return maxItemNumber - ((nextInt((int) cacheSize)));
			} else {
				// return maxItemNumber > 0?getNextGetOffset(maxItemNumber):0;//
				// Math.abs(r.nextLong()) % maxItemNumber: 0;
				// return maxItemNumber > 0?Math.abs(r.nextLong()) %
				// maxItemNumber: 0;
				return maxItemNumber > 0 ? Math
						.abs(nextInt((int) maxItemNumber)) : 0;
			}
		}

		/** To speed get request - pre- construct. */
		List<byte[]> families = Arrays.asList( new byte[][]{FAMILIES[0]});
		
		/** The columns. */
		List<byte[]> columns  = Arrays.asList( new byte[][]{COLUMNS[0]});
		
		/** The map. */
		Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);
		/**
		 * Inner loop.
		 * 
		 * @throws NativeMemoryException
		 *             the j emalloc exception
		 */
		private final void innerLoop() throws NativeMemoryException {

			long tt1 = System.nanoTime();
			//monkeyCall(tt1);
			Random r = new Random();
			boolean isReadRequest = isReadRequest();// f > sWriteRatio;
			
			if (isReadRequest) {
				try {
					long l = getNextGetIndex();

					byte[] row = Arrays.copyOf(sRow, sRow.length);
					patchRow(row, 0, l);
					
					@SuppressWarnings("unused")
          List<KeyValue> results = getFromCache(tableA, row, map);

				} catch (Exception e) {
					//e.printStackTrace();
					LOG.error("get native call.", e);
					System.exit(-1);
				}
			} else {
				try {
					long nextSeqNumber = seqNumber.incrementAndGet();
					int rowNum = r.nextInt(M);					
					cacheRow(tableA, rowNum, nextSeqNumber);
					
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("put call.", e);
					System.exit(-1);
				}
			}
			icounter++;
			totalRequests++;
			long tt2 = System.nanoTime();
//			boolean enableTiming = (tt2 - startTime) > (100000000000L); 
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
				@SuppressWarnings("unused")
                float hitRatio = (sGets.get() != 0)?
				((float)sInCache.get())/sGets.get():0;
				rps = totalRequests*1000/(System.currentTimeMillis() - mStartTime);
				LOG.info("\n\nRPS=" + rps + "\n" + "MAX=" + max + "\nAVG="
						+ avg + "\nMEDIAN=" + median + "\n99%=" + t99
						+ "\n99.9%=" + t999 + "\n99.99%=" + t9999 + "\nSIZE="
						+ getMemAllocated() + "\nITEMS=" + getTotalItems()+"\nGETS="+getTotalRequests()+
						" HITS="+getTotalHits()+
						"\nRAW SIZE="+getRawSize()+" COMP_RATIO="+getAvgCompRatio()+"\nEVICTED="+getEvictedCount()+
						"\nEVICTION ATTEMPTS="+ getTotalEvictionAttempts()+"\nEVICTION FAILED="+getTotalFailedEvictionAttempts()+
						"\nEVICTION FATAL="+getTotalFailedFatalEvictionAttempts());


			}
		}
	}

	/**
	 * Gets the total items.
	 * 
	 * @return the total items
	 */
	public static String getTotalItems() {

		return sCache.size() + "";

	}

	/**
	 * Gets the mem allocated.
	 * 
	 * @return the mem allocated
	 */
	public static String getMemAllocated() {

			return sCache.getTotalAllocatedMemorySize() + "";


	}

	/**
	 * Gets the raw size.
	 *
	 * @return the raw size
	 */
	public static String getRawSize()
	{
		return sCache.getRawDataSize()+"";
	}
	
	/**
	 * Gets the compressed size.
	 *
	 * @return the compressed size
	 */
	public static String getCompressedSize()
	{
		return sCache.getCompressedDataSize()+"";
	}
	
	/**
	 * Gets the avg comp ratio.
	 *
	 * @return the avg comp ratio
	 */
	public static String getAvgCompRatio()
	{
			return sCache.getAverageCompressionRatio()+"";
	}
	
	/**
	 * Gets the evicted count.
	 *
	 * @return the evicted count
	 */
	public static String getEvictedCount()
	{
		return sCache.getEvictedCount()+"";
	}
	
	
	/**
	 * Gets the total eviction attempts.
	 *
	 * @return the total eviction attempts
	 */
	public static String getTotalEvictionAttempts()
	{
		return sCache.getTotalEvictionAttempts()+"";
	}
	
	/**
	 * Gets the total failed eviction attempts.
	 *
	 * @return the total failed eviction attempts
	 */
	public static String getTotalFailedEvictionAttempts()
	{
		return sCache.getFailedEvictionAttempts()+"";
	}	
	
	
	/**
	 * Gets the total failed fatal eviction attempts.
	 *
	 * @return the total failed fatal eviction attempts
	 */
	public static String getTotalFailedFatalEvictionAttempts()
	{
		return sCache.getFailedFatalEvictionAttempts()+"";		
	}
	
	/**
	 * Gets the total requests.
	 *
	 * @return the total requests
	 */
	public static String getTotalRequests()
	{
		return sCache.getTotalRequestCount()+"";
	}
	
	/**
	 * Gets the total hits.
	 *
	 * @return the total hits
	 */
	public static String getTotalHits()
	{
		return sCache.getHitCount()+"";
	}
	
	
	/**
	 * Row Cache Code.
	 *
	 * @param table the table
	 * @param rowNum the row num
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	
	/**
	 * Cache row.
	 *
	 * @param table the table
	 * @param rowNum the row num
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static void cacheRow(HTableDescriptor table, int rowNum) throws IOException
	{
		List<KeyValue> list = data.get(rowNum);
		Get get = createGet(list.get(0).getRow(), null, null, null);
		get.setMaxVersions(Integer.MAX_VALUE);
		cache.resetRequestContext();
		cache.postGet(table, get, list);				
	}
	
	/**
	 * Cache row.
	 *
	 * @param table the table
	 * @param rowNum the row num
	 * @param seqNumber the seq number
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static void cacheRow(HTableDescriptor table, int rowNum, long seqNumber) throws IOException
	{
		List<KeyValue> list = data.get(rowNum);
		byte[] row = Arrays.copyOf(sRow, sRow.length);//list.get(0).getRow();
		patchRow(row, 0, seqNumber);
		//LOG.info("PUT:["+new String(row)+"]");
		Get get = createGet(row, null, null, null);
		get.setMaxVersions(Integer.MAX_VALUE);
		cache.resetRequestContext();
		cache.postGet(table, get, list);				
	}
	
	/**
	 * Gets the from cache.
	 *
	 * @param table the table
	 * @param row the row
	 * @param map the map
	 * @return the from cache
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static List<KeyValue> getFromCache(HTableDescriptor table, byte[] row, Map<byte[], NavigableSet<byte[]>> map) throws IOException
	{
		//Map<byte[], NavigableSet<byte[]>> map = constructFamilyMap(families, columns);

		Get get = createGet(row, map, null, null);	
		get.setMaxVersions(1);
		List<KeyValue> results = new ArrayList<KeyValue>();
		cache.preGet(table, get, results);		
		//LOG.info("GET:["+new String(row)+"] result="+results.size());
		return results;
		
	}
	
	
	/**
	 * Constract family map.
	 *
	 * @param families the families
	 * @param columns the columns
	 * @return the map
	 */
	protected static Map<byte[], NavigableSet<byte[]>> constructFamilyMap(List<byte[]> families, List<byte[]> columns)
	{
		Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
		if(families == null) return map;
		NavigableSet<byte[]> colSet = getColumnSet(columns);
		for(byte[] f: families){
			map.put(f, colSet);
		}
		return map;
	}
	
	/**
	 * Gets the row.
	 *
	 * @param i the i
	 * @return the row
	 */
	static byte[] getRow (int i){
		return ("rowxxxxxxx").getBytes();
	}
	
	/**
	 * Gets the value.
	 *
	 * @param i the i
	 * @return the value
	 */
	static byte[] getValue (int i){
		return ("value"+i).getBytes();
	}
	
	
	/**
	 * Generate row data.
	 *
	 * @param i the i
	 * @return the list
	 */
	static List<KeyValue> generateRowData(int i){
		byte[] row = sRow;//getRow(i);
		byte[] value = getValue(i);
		long startTime = System.currentTimeMillis();
		ArrayList<KeyValue> list = new ArrayList<KeyValue>();
		int count = 0;
		int VERSIONS = minVersions + ( i % maxVersions);
		
		for(byte[] f: FAMILIES){
			for(byte[] c: COLUMNS){
				count = 0;
				for(; count < VERSIONS; count++){
					KeyValue kv = new KeyValue(row, f, c, startTime + (count),  value);	
					list.add(kv);
				}
			}
		}
		
		Collections.sort(list, KeyValue.COMPARATOR);
		
		return list;
	}
	
	
	/**
	 * Generate data.
	 *
	 * @param n the n
	 * @return the list
	 */
	static List<List<KeyValue>> generateData(int n)
	{
		List<List<KeyValue>> data = new ArrayList<List<KeyValue>>();
		for(int i=0; i < n; i++){
			data.add(generateRowData(i));
		}
		return data;
	}
	
	/**
	 * Creates the tables.
	 */
	protected static void createTables() {
		
		HColumnDescriptor famA = new HColumnDescriptor(FAMILIES[0]);
		famA.setValue(RConstants.ROWCACHE, "true".getBytes());

		HColumnDescriptor famB = new HColumnDescriptor(FAMILIES[1]);
		famB.setValue(RConstants.ROWCACHE, "true".getBytes());
		
		HColumnDescriptor famC = new HColumnDescriptor(FAMILIES[2]);
		famC.setValue(RConstants.ROWCACHE, "true".getBytes());
		
		tableA = new HTableDescriptor(TABLE_A);
		tableA.addFamily(famA);
		tableA.addFamily(famB);
		tableA.addFamily(famC);
		
		famA = new HColumnDescriptor(FAMILIES[0]);
		famA.setValue(RConstants.ROWCACHE, "true".getBytes());
		
		famB = new HColumnDescriptor(FAMILIES[1]);
		famB.setValue(RConstants.ROWCACHE, "true".getBytes());
		
		famC = new HColumnDescriptor(FAMILIES[2]);
		
		tableB = new HTableDescriptor(TABLE_B);
		tableB.addFamily(famA);
		tableB.addFamily(famB);
		tableB.addFamily(famC);	
		
		famA = new HColumnDescriptor(FAMILIES[0]);
		
		famB = new HColumnDescriptor(FAMILIES[1]);
		
		famC = new HColumnDescriptor(FAMILIES[2]);
		
		tableC = new HTableDescriptor(TABLE_C);
		tableC.addFamily(famA);
		tableC.addFamily(famB);
		tableC.addFamily(famC);		
		
		
	}
	
 	
 	/**
	 * Creates the get.
	 *
	 * @param row the row
	 * @param familyMap the family map
	 * @param tr the tr
	 * @param f the f
	 * @return the gets the
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static Get createGet(byte[] row, Map<byte[], NavigableSet<byte[]>> familyMap, TimeRange tr, Filter f ) throws IOException
	{
		Get get = new Get(row);
		if(tr != null){
			get.setTimeRange(tr.getMin(), tr.getMax());
		}
		if(f != null) get.setFilter(f);
		
		if(familyMap != null){
			for(byte[] fam: familyMap.keySet())
			{
				NavigableSet<byte[]> cols = familyMap.get(fam);
				if( cols == null || cols.size() == 0){
					get.addFamily(fam);
				} else{
					for(byte[] col: cols)
					{
						get.addColumn(fam, col);
					}
				}
			}
		}
		return get;
	}
	
	/**
	 * Creates the put.
	 *
	 * @param values the values
	 * @return the put
	 */
	protected static Put createPut(List<KeyValue> values)
	{
		Put put = new Put(values.get(0).getRow());
		for(KeyValue kv: values)
		{
			put.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
		}
		return put;
	}
	
	
	/**
	 * Creates the increment.
	 *
	 * @param row the row
	 * @param familyMap the family map
	 * @param tr the tr
	 * @param value the value
	 * @return the increment
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected static Increment createIncrement(byte[] row, Map<byte[], NavigableSet<byte[]>> familyMap, TimeRange tr, long value) 
	throws IOException
	{
		Increment incr = new Increment(row);
		if(tr != null){
			incr.setTimeRange(tr.getMin(), tr.getMax());
		}

		
		if(familyMap != null){
			for(byte[] fam: familyMap.keySet())
			{
				NavigableSet<byte[]> cols = familyMap.get(fam);

					for(byte[] col: cols)
					{
						incr.addColumn(fam, col, value);
					}
				
			}
		}
		return incr;		
	}
	
	/**
	 * Creates the append.
	 *
	 * @param row the row
	 * @param families the families
	 * @param columns the columns
	 * @param value the value
	 * @return the append
	 */
	protected static Append createAppend(byte[] row, List<byte[]> families, List<byte[]> columns, byte[] value){
		
		Append op = new Append(row);
		
		for(byte[] f: families){
			for(byte[] c: columns){
				op.add(f, c, value);
			}
		}
		return op;
	}
	/**
	 * Creates the delete.
	 *
	 * @param values the values
	 * @return the delete
	 */
	protected static Delete createDelete(List<KeyValue> values)
	{
		Delete del = new Delete(values.get(0).getRow());
		for(KeyValue kv: values)
		{
			del.deleteColumns(kv.getFamily(), kv.getQualifier());
		}
		return del;
	}
	
	/**
	 * Creates the delete.
	 *
	 * @param row the row
	 * @return the delete
	 */
	protected static Delete createDelete(byte[] row)
	{
		Delete del = new Delete(row);
		return del;
	}
	
	/**
	 * Creates the delete.
	 *
	 * @param row the row
	 * @param families the families
	 * @return the delete
	 */
	protected static Delete createDelete(byte[] row, List<byte[]> families)
	{
		Delete del = new Delete(row);
		for(byte[] f: families)
		{
			del.deleteFamily(f);
		}
		return del;
	}

	/**
	 * Equals.
	 *
	 * @param list1 the list1
	 * @param list2 the list2
	 * @return true, if successful
	 */
	protected static boolean equals(List<KeyValue> list1, List<KeyValue> list2)
	{
		if(list1.size() != list2.size()) return false;
		Collections.sort(list1, KeyValue.COMPARATOR);
		Collections.sort(list2, KeyValue.COMPARATOR);	
		for(int i=0; i < list1.size(); i++){
			if(list1.get(i).equals(list2.get(i)) == false) return false;
		}
		return true;
	}
	
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param family the family
	 * @return the list
	 */
	protected static List<KeyValue> subList(List<KeyValue> list, byte[] family){
		List<KeyValue> result = new ArrayList<KeyValue>();
		for(KeyValue kv : list){
			if(Bytes.equals(family, kv.getFamily())){
				result.add(kv);
			}
		}
		return result;
	}
	
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param family the family
	 * @param cols the cols
	 * @return the list
	 */
	protected static List<KeyValue> subList(List<KeyValue> list, byte[] family, List<byte[]> cols){
		List<KeyValue> result = new ArrayList<KeyValue>();
		for(KeyValue kv : list){
			if(Bytes.equals(family, kv.getFamily())){
				byte[] col = kv.getQualifier();
				for(byte[] c: cols){					
					if(Bytes.equals(col, c)){
						result.add(kv); break;
					}
				}
				
			}
		}
		return result;
	}
	
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param families the families
	 * @param cols the cols
	 * @return the list
	 */
	protected static List<KeyValue> subList(List<KeyValue> list, List<byte[]> families, List<byte[]> cols){
		List<KeyValue> result = new ArrayList<KeyValue>();
		for(KeyValue kv : list){
			for(byte[] family: families){				
				if(Bytes.equals(family, kv.getFamily())){
					byte[] col = kv.getQualifier();
					for(byte[] c: cols){					
						if(Bytes.equals(col, c)){
							result.add(kv); break;
						}
					}				
				}
			}
		}
		return result;
	}
	
	
	/**
	 * Sub list.
	 *
	 * @param list the list
	 * @param families the families
	 * @param cols the cols
	 * @param max the max
	 * @return the list
	 */
	protected static List<KeyValue> subList(List<KeyValue> list, List<byte[]> families, List<byte[]> cols, int max){
		List<KeyValue> result = new ArrayList<KeyValue>();
		for(KeyValue kv : list){
			for(byte[] family: families){				
				if(Bytes.equals(family, kv.getFamily())){
					byte[] col = kv.getQualifier();
					for(byte[] c: cols){					
						if(Bytes.equals(col, c)){
							result.add(kv); break;
						}
					}				
				}
			}
		}
		
		int current = 0;
		byte[] f = result.get(0).getFamily();
		byte[] c = result.get(0).getQualifier();
		
		List<KeyValue> ret = new ArrayList<KeyValue>();
		
		for(KeyValue kv : result ){
			byte[] fam = kv.getFamily();
			byte[] col = kv.getQualifier();
			if(Bytes.equals(f, fam) ){
				if( Bytes.equals(c, col)){
					if( current < max){
						ret.add(kv);
					}
					current++;
				} else{
					c = col; current = 1;
					ret.add(kv);
				}
			} else {
				f = fam; c = col; current = 1;
				ret.add(kv);
			}
		}
		return ret;
	}
	/**
	 * Gets the column set.
	 *
	 * @param cols the cols
	 * @return the column set
	 */
	protected static NavigableSet<byte[]> getColumnSet(List<byte[]> cols)
	{
		if(cols == null) return null;
		TreeSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
		for(byte[] c: cols){
			set.add(c);
		}
		return set;
	}
	
	/**
	 * Dump.
	 *
	 * @param list the list
	 */
	protected static void dump(List<KeyValue> list)
	{
		for( KeyValue kv : list){
			dump(kv);
		}
	}
	
	/**
	 * Dump.
	 *
	 * @param kv the kv
	 */
	protected static void dump(KeyValue kv)
	{
		LOG.info("row="+new String(kv.getRow())+" family="+ new String(kv.getFamily())+
				" column="+new String(kv.getQualifier()) + " ts="+ kv.getTimestamp());
	}
	
	/**
	 * Patch row.
	 *
	 * @param kv the kv
	 * @param patch the patch
	 */
	protected static void patchRow(KeyValue kv, byte[] patch)
	{
		int off = kv.getRowOffset();
		System.arraycopy(patch, 0, kv.getBuffer(), off, patch.length);
	}	
	
	/**
	 * Patch row.
	 *
	 * @param row the row
	 * @param off the off
	 * @param seqNumber the seq number
	 */
	protected static void patchRow(byte[] row, int off, long seqNumber)
	{
		//int zeroBytes = Long.numberOfLeadingZeros( seqNumber) >> 3;
		//String s = Long.toString(seqNumber);
		//byte[] patch = s.getBytes();
		//System.arraycopy(patch, 0,row, off, patch.length);
		
		row[off] = (byte) ((seqNumber >>> 56) & 0xff); 
		row[off + 1] = (byte) ((seqNumber >>> 48) & 0xff);
		row[off + 2] = (byte) ((seqNumber >>> 40) & 0xff);
		row[off + 3] = (byte) ((seqNumber >>> 32) & 0xff);
		row[off + 4] = (byte) ((seqNumber >>> 24) & 0xff);	
		row[off + 5] = (byte) ((seqNumber >>> 16) & 0xff);	
		row[off + 6] = (byte) ((seqNumber >>> 8) & 0xff);
		row[off + 7] = (byte) ((seqNumber ) & 0xff);	


		
	}	
	
}


