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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.log4j.Logger;

import com.koda.IOUtils;
import com.koda.NativeMemoryException;
import com.koda.io.serde.SerDe;
import com.koda.util.SpinLock;
import com.koda.util.SpinReadWriteLock;

// TODO: Auto-generated Javadoc
/**
 * The Class Scanner.
 * 
 * TODO: large memory buffers > 2G
 */
public class CacheScanner extends AbstractScannerImpl{
	
	public static enum Mode {READ, UPDATE}
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(CacheScanner.class);
	
	/** The Constant BUFFER_SIZE. */
	private int BUFFER_SIZE = 256;
	
	/** The off-heap cache. */
	OffHeapCache mCache;

	/** Internal buffer. */
	long[] mInternalBuffer = null;//new long[BUFFER_SIZE];
	
	/** Internal buffer index. */
	int mBufferIndex =0;
	
	/** Last processed index. */
	
	int mLastProcessedIndex;
	
	/** The scanner's number - starts with 0. */
	int mScannerNumber;
	
	/** The total number of scanners. */
	int mTotalScanners;
		
	/** The m current index. */
	long mCurrentIndex;

	/** The m start index. */
	long mStartIndex; // inclusive

	/** The m end index. */
	long mEndIndex; // inclusive


	/** The m stride. */
	int mStride;

	/** The m current ptr. */
	long mCurrentPtr;

	/** The m locks. */
	SpinReadWriteLock[] mLocks;

	/** The m buffer. */
  long mMemPointer;
    
	/** The lock. */
	SpinReadWriteLock lock;

	/** The read lock. */
	Lock opLock;

	Mode mMode = Mode.READ; // by dea
	
	/** Spin lock for disposed cache. */
	SpinLock mSpinLock = new SpinLock();

	/** The m is closed. */
	private boolean mIsClosed = false;
	
	private boolean needLock = true;
    
	
	
	/**
	 * Instantiates a new scanner.
	 *
	 * @param cache the cache
	 * @param scanNumber the scan number
	 * @param totalScanners the total scanners
	 */
	public CacheScanner(OffHeapCache cache, int startIndex, int stopIndex, int dummy) {
		this.mCache = cache;

		this.mStride = OffHeapCache.getLockStripesCount();
		this.mLocks = mCache.getLocks();
		mInternalBuffer = new long[BUFFER_SIZE];
		this.mMemPointer = mCache.getMemPointer();

		this.mStartIndex = startIndex;
		this.mEndIndex = stopIndex;
		this.mCurrentIndex = mStartIndex;
		SpinReadWriteLock lock = mCache.getLock(mCurrentIndex);
		ReadLock readLock = lock.readLock();
		readLock.lock();
		try {
			// initialize current pointer
			mCurrentPtr = IOUtils.getLong(mMemPointer, mCurrentIndex * 8);
		} finally {
			readLock.unlock();
		}

	}
	
	/**
	 * Instantiates a new scanner.
	 *
	 * @param cache the cache
	 * @param scanNumber the scan number
	 * @param totalScanners the total scanners
	 */
	CacheScanner(OffHeapCache cache, int scanNumber, int totalScanners) {
		this.mCache = cache;
		this.mScannerNumber = scanNumber;
		this.mTotalScanners = totalScanners;
    mInternalBuffer = new long[BUFFER_SIZE];

		this.mStride = OffHeapCache.getLockStripesCount();
		this.mLocks = mCache.getLocks();
		//this.mBuffer = mCache.getOffHeapBuffer();
		this.mMemPointer = mCache.getMemPointer();
		calculateIndexRange();
		this.mCurrentIndex = mStartIndex;
		SpinReadWriteLock lock = mCache.getLock(mCurrentIndex);
		ReadLock readLock = lock.readLock();
		readLock.lock();
		try {
			// initialize current pointer
			mCurrentPtr = IOUtils.getLong(mMemPointer, mCurrentIndex * 8);
		} finally {
			readLock.unlock();
		}

	}
	
	CacheScanner(OffHeapCache cache, int scanNumber, int totalScanners, boolean needLock) {
		this.mCache = cache;
		this.mScannerNumber = scanNumber;
		this.mTotalScanners = totalScanners;
    mInternalBuffer = new long[BUFFER_SIZE];

		this.mStride = OffHeapCache.getLockStripesCount();
		this.mLocks = mCache.getLocks();
		//this.mBuffer = mCache.getOffHeapBuffer();
		this.mMemPointer = mCache.getMemPointer();
		calculateIndexRange();
		this.mCurrentIndex = mStartIndex;
		SpinReadWriteLock lock = mCache.getLock(mCurrentIndex);
		
		this.needLock = needLock;
		
		ReadLock readLock = lock.readLock();
		readLock.lock();
		try {
			// initialize current pointer
			mCurrentPtr = IOUtils.getLong(mMemPointer, mCurrentIndex * 8);
		} finally {
			readLock.unlock();
		}

	}
	
	private Lock getOperationLock(SpinReadWriteLock lock)
	{
	  switch (mMode){
	    case READ: return lock.readLock();
	    case UPDATE:  return lock.writeLock();
	  }
	  return null;
	}
	
	/**
	 * Gets the cache.
	 *
	 * @return the cache
	 */
	public OffHeapCache getCache()
	{
		return mCache;
	}
	
	
	/**
	 * Calculate index range.
	 */
	private void calculateIndexRange() {
		long n = mCache.getTotalBuckets();
		long d = n / mTotalScanners;
		if (d * mTotalScanners < n) {
			if (mScannerNumber < mTotalScanners - 1) {
				mStartIndex = (d + 1) * mScannerNumber;
				mEndIndex = (d + 1) * (mScannerNumber + 1) - 1;
			} else {
				mStartIndex = (d + 1) * (mScannerNumber);
				mEndIndex = n - 1;
			}

		} else {
			mStartIndex = d * mScannerNumber;
			mEndIndex = d * (mScannerNumber + 1) - 1;
		}
		if (mEndIndex >= n)
			mEndIndex = n - 1;

		mCurrentIndex = mStartIndex;
		
		LOG.debug("Scanner["+mScannerNumber+"]: start =" + mStartIndex + " end=" + mEndIndex);
	}

	/**
	 * To make Iterator interface.
	 *
	 * @return true, if there are elements in the scanner
	 */
	public boolean hasNext()
	{
		try{
			return nextPointer() != 0L;
		}finally{
			mBufferIndex--;
		}
	}
	
	public void setNeedLock(boolean b)
	{
	  this.needLock = b;
	}
	
	
	
	/**
	 * Next record's pointer. The current pointer is guarded and safe until the next call to 
	 * nextPointer method is done. 
	 * 
	 * @return the pointer to the next record; 0 if there are no records left
	 */

	
	public final long nextPointer()
	{
		if(mBufferIndex < mInternalBuffer.length && mInternalBuffer[mBufferIndex] != 0)
		{
			long ptr = mInternalBuffer[mBufferIndex ++];
			if(mPrefetchEnabled){
				if(mBufferIndex+mPrefetchInterval < mInternalBuffer.length){
					prefetch(mInternalBuffer[mBufferIndex+mPrefetchInterval], mPrefetchType.ordinal(),
						mLocality.ordinal(), mPrefetchLines);
				}
			}
			return ptr;
		}
		
		// Fill index array
		if( mCurrentIndex > mEndIndex) {
			// close scanner
			close();
			return 0L;// We are done
		}
		mBufferIndex = 0;
		// Get next safe
		checkLock();
		
		
		if( needLock){
			SpinReadWriteLock otherLock = mCache.getLock(mCurrentIndex);
			if (otherLock != lock) {
				opLock.unlock();
				unlocked++;
				lock = otherLock;
				opLock = getOperationLock(lock); //lock.readLock();
				//readLock.lock();
				try {
					if(!opLock.tryLock(10000000, TimeUnit.MILLISECONDS)){
					  
						throw new RuntimeException("Can not acquire read lock");
					}
					locked++;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					LOG.warn(e);
				}
			}
		}
		
		mInternalBuffer[0] = 0L;
		while(mInternalBuffer[0] == 0L && mCurrentIndex <= mEndIndex){
			long end = getSafeRangeEnd(mCurrentIndex);
			end = Math.min(end, mEndIndex);
			//LOG.info(" loop body mEndIndex="+mEndIndex+ " scanner="+this);
			mCurrentIndex = scanFill(mMemPointer, mCurrentIndex, end, mInternalBuffer) +1;
		}

		return mInternalBuffer[mBufferIndex++];
	}

	
	/**
	 * This is not a public API and it meant to be used only
	 * when scanning disposed caches.
	 * @return memory pointer
	 */
	final long nextPointerSafe() {
		
		if(needLock) while (!mSpinLock.lock());
			
		try {

			if (mBufferIndex < mInternalBuffer.length
					&& mInternalBuffer[mBufferIndex] != 0) {
				long ptr = mInternalBuffer[mBufferIndex++];
				if (mPrefetchEnabled) {
					if (mBufferIndex + mPrefetchInterval < mInternalBuffer.length) {
						prefetch(mInternalBuffer[mBufferIndex
								+ mPrefetchInterval], mPrefetchType.ordinal(),
								mLocality.ordinal(), mPrefetchLines);
					}
				}
				return ptr;
			}

			// Fill index array
			if (mCurrentIndex > mEndIndex) {
				// close scanner
				//close();
				return 0L;// We are done
			}
			mBufferIndex = 0;
			mInternalBuffer[0] = 0L;
			while (mInternalBuffer[0] == 0L && mCurrentIndex <= mEndIndex) {
				long end = getSafeRangeEnd(mCurrentIndex);
				end = Math.min(end, mEndIndex);
				mCurrentIndex = scanFill(mMemPointer, mCurrentIndex, end,
						mInternalBuffer) + 1;
			}
			return mInternalBuffer[mBufferIndex++];
		} finally {
			if(needLock) mSpinLock.unlock();
		}
	}
	
	/**
	 * Safe range end is the last bucket number which is protected
	 * by the same lock as the current bucket index.
	 *
	 * @param index the index
	 * @return end of the range assigned to a current lock
	 */
	private long getSafeRangeEnd(long index) {
		long total = mCache.getTotalBuckets();
		int stripeCount = OffHeapCache.getLockStripesCount();
		int bucketWidth = (int)(total/stripeCount);
		int lockIndex = (int)(index/bucketWidth);
		return (lockIndex < stripeCount -1)?(lockIndex+1)*bucketWidth-1: total-1;
	}

	/**
	 * Check lock.
	 */
	private void checkLock()
	{
		if( needLock == false) return;
		if (lock == null) {
			lock = mCache.getLock(mCurrentIndex);
			opLock = getOperationLock(lock);//lock.readLock();
			try {
				if(!opLock.tryLock(60000, TimeUnit.MILLISECONDS)){
				  // FIXME: hard-coded 60 sec timeout
					throw new RuntimeException("Can not acquire read lock");
				}
				locked++;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				LOG.warn(e);
			}
		}
	}
	
	/**
	 * Close the scanner. This method SHOULD always be called for safety
	 * reason. It is not necessary to call this method if the end of stream is reached
	 */
	
	public static long locked = 0;
	public static long unlocked = 0;
	public void close() {

		synchronized(this){
			if(mIsClosed) return;
			try {
				if (needLock && opLock != null ){
					opLock.unlock(); unlocked++;
				}
				// TODO some dispose method for OffHeapCache
				mCache = null;
				mIsClosed  = true;
			} catch (Exception e) {
				LOG.warn(e);
			}
		}
	}

	/**
	 * Reads next key into existing array.
	 *
	 * @param buffer the buffer
	 * @return lengh of a key;  0 if end of scanner's stream
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public int nextKey(byte[] buffer) throws NativeMemoryException
	{
		long ptr = nextPointer();
		if(ptr == 0) return 0;
		return OffHeapCache.getKeyDirectBytes(ptr, buffer);
	}
		
	
	/**
	 * Gets next key from scanner's stream.
	 *
	 * @return the key or null
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public byte[] nextKeyByteArray() throws NativeMemoryException
	{
		long ptr = nextPointer();
		if(ptr == 0) return null;
		return OffHeapCache.getKeyDirectNewBytes(ptr);
	}
	
	
	/**
	 * Gets next key from scanner stream into existing ByteBuffer. This method is unsafe.
	 * This method does not check byte buffer capacity. 
	 * @param buffer the buffer
	 * @return true if successfull, false if end of stream
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean nextKey(ByteBuffer buffer) throws NativeMemoryException
	{
		long ptr = nextPointer();
		if(ptr == 0) return false;		
		OffHeapCache.getKey(ptr, buffer);
		return true;
	}
	
	/**
	 * Next key into existing ByteBuffer (by a given address). This method is unsafe.
	 * You should first get ByteBuffer address using NativeMemory.getBufferAddress
	 * @param bufPtr the buffer's address
	 * @return true, if successful, false if end of stream
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean nextKey(long bufPtr) throws NativeMemoryException
	{
		long ptr = nextPointer();
		if(ptr == 0) return false;
		OffHeapCache.getKeyDirect(ptr, bufPtr);
		return true;
	}
	
	/**
	 * Next record into existing array: both key and value
	 * Record format:
	 * 0-3 - key length
	 * 4-7 - value length
	 * key
	 * value.
	 *
	 * @param buffer the buffer
	 * @return int the total length of a record; -1 if no record and 0 if end of scanner
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public int nextRecord(byte[] buffer) throws NativeMemoryException {
		long ptr = nextPointer();
		if(ptr == 0) return 0;
		return OffHeapCache.getRecordDirectBytes(ptr, buffer);

	}

	/**
	 * Next record (into new array).
	 *
	 * @return next record or null if not found
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public byte[] nextRecordByteArray() throws NativeMemoryException {
		long ptr = nextPointer();
		if(ptr == 0) return null;
		return OffHeapCache.getRecordDirectNewBytes(ptr);
	}

	/**
	 * Next record into byte buffer.
	 *
	 * @param buffer the buffer
	 * @return true if success, false if end of stream
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean nextRecord(ByteBuffer buffer) throws NativeMemoryException {
		long ptr = nextPointer();
		if(ptr == 0) return false;		
		OffHeapCache.getRecord(ptr, buffer);
		return true;
	}
	
	
	
	/**
	 * Next record.
	 *
	 * @return the byte buffer
	 * @throws NativeMemoryException the native memory exception
	 */
	public ByteBuffer nextRecord() throws NativeMemoryException
	{
		ByteBuffer buf = mCache.getLocalBufferWithAddress().getBuffer();
		boolean result = nextRecord(buf);
		if(result){
			return buf;
		} else{
			return null;
		}
	}
	
	/**
	 * Gets the whole record (key-value pair) into
	 * an existing byte buffer given by buffers address.
	 *
	 * @param bufPtr the buf ptr
	 * @return true if success, false if end of stream
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean nextRecord(long bufPtr) throws NativeMemoryException
	{
		long ptr = nextPointer();
		if(ptr == 0) return false;		
		OffHeapCache.getRecordDirect(ptr, bufPtr);
		return true;
	}
	
	/**
	 * Next value into existing array.
	 *
	 * @param buffer the buffer
	 * @return int the total length of a value;  0 if end of scanner
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public int nextValue(byte[] buffer) throws NativeMemoryException {
		long ptr = nextPointer();
		if(ptr == 0) return 0;
		return OffHeapCache.getValueDirectBytes(ptr, buffer);
	}

	/**
	 * Next value (into new array).
	 *
	 * @return next record or null if not found
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public byte[] nextValueByteArray() throws NativeMemoryException {
		long ptr = nextPointer();
		if(ptr == 0) return null;
		return OffHeapCache.getValueDirectNewBytes(ptr);
	}

	/**
	 * Next value into byte buffer.
	 *
	 * @param buffer the buffer
	 * @return length of a value; -1 - no more records or 0 if does not fit
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean nextValue(ByteBuffer buffer) throws NativeMemoryException {
		long ptr = nextPointer();
		if(ptr == 0) return false;		
		OffHeapCache.getValue(ptr, buffer);
		return true;
	}
	
	
	/**
	 * Next value.
	 *
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object nextValue() throws NativeMemoryException, IOException
	{
		long ptr = nextPointer();
		if(ptr == 0L) return null;
		ByteBuffer buf = mCache.getLocalBufferWithAddress().getBuffer();
		OffHeapCache.getValue(ptr, buf);
		SerDe serde =  mCache.getSerDe();
		buf.position(4);
		return serde.readCompressed(buf/*, mCache.getCompressionCodec()*/);
	}
	
	 public Object value(long ptr) throws NativeMemoryException, IOException
	  {
	    if(ptr == 0L) return null;
	    ByteBuffer buf = mCache.getLocalBufferWithAddress().getBuffer();
	    OffHeapCache.getValue(ptr, buf);
	    SerDe serde =  mCache.getSerDe();
	    buf.position(4);
	    return serde.readCompressed(buf/*, mCache.getCompressionCodec()*/);
	  }
	/**
	 * Next key.
	 *
	 * @return the object
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object nextKey() throws NativeMemoryException, IOException
	{
		long ptr = nextPointer();
		if(ptr == 0L) return null;
		ByteBuffer buf = mCache.getLocalBufferWithAddress().getBuffer();
		OffHeapCache.getKey(ptr, buf);
		SerDe serde =  mCache.getSerDe();
		buf.position(4);
		return serde.read(buf);
	}
	
	 public Object key(long ptr) throws NativeMemoryException, IOException
	  {

	    if(ptr == 0L) return null;
	    ByteBuffer buf = mCache.getLocalBufferWithAddress().getBuffer();
	    OffHeapCache.getKey(ptr, buf);
	    SerDe serde =  mCache.getSerDe();
	    buf.position(4);
	    return serde.read(buf);
	  }
	
	/**
	 * Gets the whole record (key-value pair) into
	 * an existing byte buffer given by buffers address.
	 *
	 * @param bufPtr the buf ptr
	 * @return true if success, false if end of stream
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public boolean nextValue(long bufPtr) throws NativeMemoryException
	{
		long ptr = nextPointer();
		if(ptr == 0) return false;		
		OffHeapCache.getValueDirect(ptr, bufPtr);
		return true;
	}
	
	public void setOperationMode(Mode mode)
	{
	  mMode = mode;
	}
	
	public Mode getOperationMode()
	{
	  return mMode;
	}

}
