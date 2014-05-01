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
package com.koda.integ.hbase.test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.cache.eviction.EvictionAlgo;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;
import com.koda.integ.hbase.blockcache.OffHeapBlockCacheOld;
import com.koda.integ.hbase.storage.ExtStorageManager;
import com.koda.integ.hbase.storage.FileExtStorage;
import com.koda.integ.hbase.storage.FileStorageHandle;
import com.koda.integ.hbase.storage.StorageRecycler;
import com.koda.integ.hbase.util.StorageHandleSerializer;
import com.koda.util.Utils;

// TODO: Auto-generated Javadoc
/**
 * The Class OffHeapHashMapPerfTest.
 */
public class FileStoragePerfTest {



	/** The Constant THREADS. */
	private final static String THREADS = "-t";

	/** The Constant BUCKETS. */
	//private final static String BUCKETS = "-b"; // koda specific

	/** The Constant MAXMEMORY. */
	//private final static String MAXMEMORY = "-mm";

	/** The Constant MAXITEMS. */
	//private final static String MAXITEMS = "-mi";

	/** The Constant WRITE_RATIO. */
	private final static String WRITE_RATIO = "-writes"; // write ops %%


	/** The Constant DURATION. */
	private final static String DURATION = "-duration";



	/** The N. */
	public static int N = 1024 * 1024;

	/** The Constant LOG. */

	private final static Logger LOG = Logger.getLogger(FileStoragePerfTest.class);

	/** The base dir. */
	static String baseDir = "/tmp/ramdisk/data";
	
	/** The Koda cache instance, which keeps storage references. */
	private static OffHeapCache sCache;

	/** The s storage. */
	private static FileExtStorage sStorage;
	
	/** The s test time. */
	private static long sTestTime = 6000000;// 600 secs

	/** The s write ratio. */
	private static float sWriteRatio = 0.1f; // 10% puts - 90% gets

	/** The s interval. */
	private static long sInterval = 5000;

	/** Memory limit in bytes - used for Koda. */
	//private static long sMemoryLimit = 1000000000L; // 1G by default

	/** Cache items limit - used for all other caches. */
	//private static long sCacheItemsLimit = 1500000; // 1.5 M by default

	/** Number of client threads. */
	private static int sClientThreads = 2; // by default

	/** The s puts. */
	private static AtomicLong sPuts = new AtomicLong(0);

	/** The s gets. */
	private static AtomicLong sGets = new AtomicLong(0);

	/** The s in cache. */
	private static AtomicLong sInCache = new AtomicLong(0);

	/** The buffer. */
	static ThreadLocal<ByteBuffer> bufferTLS = new ThreadLocal<ByteBuffer>(){

		/* (non-Javadoc)
		 * @see java.lang.ThreadLocal#initialValue()
		 */
		@Override
		protected ByteBuffer initialValue() {
			return ByteBuffer.allocateDirect(4*1024*1024);
		}
		
		
	};
	
	/**
	 * Sets the up.
	 *
	 * @throws Exception the exception
	 */
	protected static void setUp() throws Exception {

		Configuration config = new Configuration();
		config.set(FileExtStorage.FILE_STORAGE_BASE_DIR, baseDir);
		config.set(FileExtStorage.FILE_STORAGE_MAX_SIZE, "1000000000");
		config.set(OffHeapBlockCacheOld.EXT_STORAGE_IMPL, "com.koda.integ.hbase.storage.FileExtStorage");
		// 10MB file size limit
		config.set(FileExtStorage.FILE_STORAGE_FILE_SIZE_LIMIT,"50000000");
		// 2MB buffer size 
		config.set(FileExtStorage.FILE_STORAGE_BUFFER_SIZE,Integer.toString(8*1024*1024));
		
		config.set(FileExtStorage.FILE_STORAGE_NUM_BUFFERS, "2");
		
		config.set(StorageRecycler.STORAGE_RATIO_LOW_CONF, "0.85");
		
		config.set(StorageRecycler.STORAGE_RATIO_HIGH_CONF, "0.9");
		
		config.set(FileExtStorage.FILE_STORAGE_PAGE_CACHE, Boolean.toString(false));
		
		checkDir();
		

		
		CacheManager manager = CacheManager.getInstance();	
		CacheConfiguration cfg = new CacheConfiguration();
		// Max memory = 100MB
		cfg.setMaxMemory(100000000);
		cfg.setEvictionPolicy("LRU");
		// Bucket number 
		cfg.setBucketNumber(2000000);
		cfg.setCodecType(CodecType.LZ4);
		sCache = manager.createCache(cfg);
		// SerDe
		StorageHandleSerializer serde2 = new StorageHandleSerializer();
		sCache.getSerDe().registerSerializer(serde2);
		sStorage = (FileExtStorage) ExtStorageManager.getInstance().getStorage(config, sCache);
		

	}
	
	/**
	 * Check dir.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void checkDir() throws IOException
	{
		File dir = new File(baseDir);
		TestUtils.delete(dir);
		dir.mkdirs();
	}
	
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	protected static byte[] getValue()
	{
		ByteBuffer buffer = bufferTLS.get();
		int size =buffer.getInt(0);
		if( size <=0 ) return null;
		byte[] bytes = new byte[size];
		buffer.limit(size + 4);
		buffer.position(4);
		buffer.get(bytes);
		return bytes;
	}
	
	/**
	 * Put value.
	 *
	 * @param block the block
	 */
	protected static void putValue(byte[] block)
	{
		ByteBuffer buffer = bufferTLS.get();
		buffer.clear();
		buffer.putInt(block.length);
		buffer.put(block);
		buffer.flip();
	}	
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
		setUp();

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

			EvictionAlgo algo = sCache.getEvictionAlgo();
			LOG.info("Eviction stats:");
			LOG
					.info("  number of attempts ="
							+ algo.getTotalEvictionAttempts());
			LOG.info("  number of evicted  =" + algo.getTotalEvictedItems());

		LOG.info("Estimated RPS="
				+ ((double) (sPuts.get() + sGets.get()) * 1000) / (t2 - t1));
		
		System.exit(0);

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
			} /* else if (args[i].equals(BUCKETS)) {
				N = Integer.parseInt(args[++i]);
			} else if (args[i].equals(MAXMEMORY)) {
				sMemoryLimit = Long.parseLong(args[++i]);
			} else if (args[i].equals(MAXITEMS)) {
				sCacheItemsLimit = Long.parseLong(args[++i]);
			} */else if (args[i].equals(WRITE_RATIO)) {
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
		//private long startTime = System.nanoTime();
		
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
		//byte[] valueHolder;
		
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
			LOG.info("File Storage Performance test. Cache size =" + sCache.size()
					+ ": " + Thread.currentThread().getName());

			// TODO init Kode cache

			buf = NativeMemory.allocateDirectBuffer(256, 100000);
			bufPtr = NativeMemory.getBufferAddress(buf);

			// There is 100 different values between 200 -800
			// Creates values;
			values = new byte[1000][];
			for (int i = 0; i < 1000; i++) {
				values[i] = new byte[r.nextInt(1000) + 10000];
				//values[i] = new byte[4];
				byte v = (byte)(values[i].length % 111);
				for(int k=0; k < values.length; k++) values[i][k] = v;
			}

			//valueHolder = new byte[20000];
			
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
					boolean result = innerLoop();
					if(result == false) break;
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

			long cacheSize = /* (sCacheItemsLimit > 0) ? sCacheItemsLimit : */sCache
					.size();
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

		/** The total reads. */
		static AtomicInteger totalReads = new AtomicInteger(0);
		
		/** The success reads. */
		static AtomicInteger successReads = new AtomicInteger(0);
		
		/** The handle null reads. */
		static AtomicInteger handleNullReads = new AtomicInteger(0);
		
		/** The block null reads. */
		static AtomicInteger blockNullReads = new AtomicInteger(0);
		
		/**
		 * Inner loop.
		 *
		 * @return true, if successful
		 * @throws NativeMemoryException the j emalloc exception
		 */
		private final boolean innerLoop() throws NativeMemoryException {

			long tt1 = System.nanoTime();
			//monkeyCall(tt1);
			ByteBuffer buffer = bufferTLS.get();
			boolean isReadRequest = isReadRequest();// f > sWriteRatio;
			if (isReadRequest) {
				try {
					long l = getNextGetIndex();
					totalReads.incrementAndGet();
					byte[] bkey = getKeyLong(l, keyBuf);
					FileStorageHandle handle = (FileStorageHandle) sCache.get(bkey);
					if(handle != null){ 
						sStorage.getData(handle, buffer);					
						byte[] block = getValue();
						if( block != null){
							int size = Bytes.toInt(block, 0);
							if( size != block.length){
								LOG.fatal("Size = "+size+" - wrong. Real size="+(buffer.getInt(0))+" handle="+handle);
								LOG.info("Total reads ="+totalReads.get()+" success ="+successReads.get()+
										" hnadleNull="+handleNullReads.get()+" block null="+blockNullReads.get());
								//LOG.info("Expected value="+ (block.length % 111));
								//for(int i= 0; i < block.length; i ++) System.out.print(block[i]+" ");
								//System.exit(0);
								
								String fileName = sStorage.getFilePath(handle.getId());
								verifyFile(fileName, handle.getOffset(), handle.getSize());

//								return false;
								
								int numAttempts = 10;
								int attempt = 0;
								while(attempt ++ < numAttempts){
									Thread.sleep(1);
									buffer.clear();
									sStorage.getData(handle, buffer);					
									block = getValue();
									if( block != null){
										size = Bytes.toInt(block, 0);
										if(size == block.length) break;
										LOG.fatal("Attempt="+attempt+" Size = "+size+" - wrong. Real size="+(buffer.getInt(0))+" handle="+handle);
									}
								}
								if(attempt >= numAttempts){
									System.exit(-1);
								}
								
							} else{
								successReads.incrementAndGet();
							}
						} else{
							blockNullReads.incrementAndGet();
						}
					} else{
						handleNullReads.incrementAndGet();
					}
				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("get native call.", e);
					System.exit(-1);
				}
			} else {
				try {
					int i = r.nextInt(1000);
					byte[] bkey = getKeyLong(maxItemNumber++, keyBuf);
					byte[] value = values[i];
					//Bytes.putInt(value, 0,  value.length);					
					putValue(value);
					// update buffer's 4-7 bytes with value size
					buffer.putInt(4, value.length);
					FileStorageHandle handle = (FileStorageHandle) sStorage.storeData(buffer);										
					sCache.put(bkey, handle);

				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("put call.", e);
					System.exit(-1);
				}
			}
			icounter++;
			totalRequests++;
			long tt2 = System.nanoTime();
			//boolean enableTiming = (tt2 - startTime) > (100000000000L); 
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
			return true;
		}
	
	
		/**
		 * Verify file.
		 *
		 * @param fileName the file name
		 * @param offsetTo the offset to
		 * @param sizeTo the size to
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		private void verifyFile(String fileName, int offsetTo, int sizeTo) throws IOException{
			LOG.info("File: "+fileName+" verification starts");
			RandomAccessFile file = new RandomAccessFile(fileName,"r");
			FileChannel fc = file.getChannel();
			
			MappedByteBuffer buffer = fc.map(MapMode.READ_ONLY, 0, file.length());
			
			boolean offsetFound = false;
			
			try{
			while(buffer.hasRemaining()){
				int size = buffer.getInt();
				if( buffer.position() == offsetTo + 4 && size == sizeTo){
					offsetFound = true;
				}
				if(size < 10000 || size > 11000){
					LOG.fatal("File: "+fileName+" corrupted at "+(buffer.position() -4)+ "size ="+ size+" file size="+file.length() );
					buffer.position(0);
					//LOG.info();
					return;
				} else{
					// skip 'size -4'
					int pos = buffer.position();
					buffer.position(pos + size -4);
				}
			}
			} finally{
				fc.close();
				file.close();
			}
			LOG.info("File: "+fileName+" - OK. offset "+offsetTo+ " size "+sizeTo+" found ="+offsetFound);
			
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
			if(sStorage != null)
			return sStorage.getCurrentStorageSize()+"";
			return "N/A";
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

		{
			return sCache.getAverageCompressionRatio()+"";
		}
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
	

}

