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
package com.koda.persistence.rawfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.koda.IOUtils;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.cache.OffHeapCache;
import com.koda.compression.Codec;
import com.koda.config.DiskStoreConfiguration;
import com.koda.io.Worker;

// TODO: Auto-generated Javadoc
/**
 *  1. Pool of threads (workers)
 *  2. One I/O thread per DataRoot
 *  3. Pool of free buffers (blocking queue)
 *  4. Pool of filled buffers (blocking queue)
 *  5. I/O thread gets buffers from filled buffers
 *  6 I/O thread puts buffers into free pool
 *  7 Worker threads get buffers from free pool
 *  8 Worker threads put buffers into filled pool
 *  9 Main thread spawn workers and IOs and wait for all 
 *    workers to complete, then interrupts all IO threads 
 * 
 * 
 * 
 * 1. Large buffers (> 2G objects)
 */

public class SaveThread extends Worker<ByteBuffer>
{

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(SaveThread.class);
	/** The cache. */
	private OffHeapCache memStore;
	
	/** The start index. */
	private long startIndex; // inclusive
	
	/** The stop index. */
	private long stopIndex;  // exclusive
	
	/** The current index. */
	private long currentIndex;

	/** The ignore expired. */
	private boolean ignoreExpired = false;
	
	/** The temp buffer. */
	private ByteBuffer tempBuffer; 
	
	/** The codec. */
	private Codec codec;
	
	/**
	 * Instantiates a new save thread.
	 *
	 * @param workerNumber the worker number
	 * @param totalWorkers the total workers
	 * @param cache the cache
	 * @param from the from
	 * @param to the to
	 */
	public SaveThread(int workerNumber, int totalWorkers, OffHeapCache cache, BlockingQueue<ByteBuffer> from,
			BlockingQueue<ByteBuffer> to) {
		super("SaveThread#"+workerNumber, from, to);
		this.memStore = cache;

		// calculate indexes
		long totalBuckets = cache.getTotalBuckets();
		
		long totalPerWorker = totalBuckets/totalWorkers +1;
		startIndex = workerNumber * totalPerWorker;
		stopIndex = startIndex + totalPerWorker;
		if(stopIndex > totalBuckets) {
			stopIndex = totalBuckets;
		}
		currentIndex = startIndex; 

		DiskStoreConfiguration cfg =memStore.getCacheConfiguration().getDiskStoreConfiguration();
		int bufferSize = ((RawFSConfiguration) cfg).getRWBufferSize(); 
		//LOG.info("Buffer size="+bufferSize);
		tempBuffer = ByteBuffer.allocateDirect(bufferSize);
		tempBuffer.order(ByteOrder.nativeOrder());
		

    codec = memStore.getCacheConfiguration().getDiskStoreConfiguration().
        											getDbCompressionType().getCodec();
    //LOG.info("Codec ="+codec);

	}

	
	/**
	 * Sets the ignore expired.
	 *
	 * @param value the new ignore expired
	 */
	public void setIgnoreExpired(boolean value)
	{
		this.ignoreExpired = value;
	}
	

	
	
	/* (non-Javadoc)
	 * @see com.koda.io.Worker#finish()
	 */
	@Override
	protected void finish() {
		// Do nothing	
		LOG.info(Thread.currentThread().getName()+" finished.");
	}

	/* (non-Javadoc)
	 * @see com.koda.io.Worker#process(java.lang.Object)
	 */
	
	@Override
	protected boolean process(ByteBuffer buffer) throws IOException {

		buffer.clear();
		try {
			long old = currentIndex;
			if (codec != null) {
				tempBuffer.clear();
				currentIndex = fillBuffer(tempBuffer, memStore, currentIndex,
						ignoreExpired);
			} else {
				currentIndex = fillBuffer(buffer, memStore, currentIndex,
						ignoreExpired);
			}
			// Increase buffer size
			if (old == currentIndex) {			  
				throw new IOException("Increase buffer size. Buffer overflow");
			}

			buffer.flip();
			if (codec != null) {
           		tempBuffer.flip();
				// compress block
				// skip blobSize
				tempBuffer.position(4);

				buffer.clear();
				buffer.position(4);
				codec.compress(tempBuffer, buffer);
				buffer.putInt(0, buffer.limit() - 4);
				buffer.position(0);
			}

		} catch (Exception e) {
			LOG.error(e);
			throw new IOException(e);

		}

		if (currentIndex >= stopIndex) {
			return true;
		}
		return false;
	}	
	
	
	/**
	 * Fill buffer.
	 *
	 * @param buf the buf
	 * @param memStore the mem store
	 * @param current the current
	 * @param ignoreExpired the ignore expired
	 * @return the int
	 * @throws NativeMemoryException the native memory exception
	 */
	private long fillBuffer(ByteBuffer buf, OffHeapCache memStore, 
			long current,  boolean ignoreExpired ) throws NativeMemoryException {

		long bufptr = memStore.getBufferAddress();
		long limit = stopIndex; 
		buf.position(4);
				
		for(; current < limit; current++){
				long ptr =  IOUtils.getLong(bufptr, current * 8);
				if(ptr == 0L) continue;
				int remaining = buf.capacity() - buf.position();
				int bucketListSize = getBucketListSize(ptr);
				
				if(bucketListSize + 8 > remaining){
					// TODO if we return the same 'current'?
					break;
				}
				int pos = buf.position();
				buf.putLong(current);

				writeBucketList(buf, ptr, memStore, ignoreExpired);

				buf.position(pos + bucketListSize + 8);
		}
		// Write blob size
		buf.putInt(0, buf.position() -4);		
		return current;
	}
	
	/**
	 * Write bucket list.
	 *
	 * @param buf the buf
	 * @param ptr the ptr
	 * @param memStore the mem store
	 * @param ignoreExpired the ignore expired
	 * @throws NativeMemoryException the native memory exception
	 */
	private void writeBucketList(ByteBuffer buf, long ptr, OffHeapCache memStore, boolean ignoreExpired) 
		throws NativeMemoryException
	{
		// skip first 2 bytes
		int pos = buf.position();
		buf.position(pos + 2);
		int total = 0;
		int offset = pos +2;
		while(ptr != 0){
			ptr = OffHeapCache.getRealAddress(ptr);
			boolean expired = false;
			//TODO
			if(ignoreExpired){
				if(memStore.isExpiredEntry(ptr)){
					expired = true;
				}
			}
			int size = IOUtils.getRecordSize(ptr/*+OffHeapCache.OFFSET*/);
			size += OffHeapCache.OFFSET + 8;
			if(!expired) {
				NativeMemory.memcpy(ptr, 0, buf, offset, size);	
//				/*DEBUG*/for(int i = offset; i < offset + size; i++){
//				  System.out.print(buf.get(i)+" ");
//				  
//				}
//				System.out.println();
//				//long addr = NativeMemory.lockAddress(ptr);
//				for(int i=0; i < size; i++){
//				  
//				  System.out.print(IOUtils.getByte(ptr, i)+" ");
//				  
//				}
				//NativeMemory.unlockAddress(ptr);
			} 
			// next ptr
			ptr = IOUtils.getLong(ptr, OffHeapCache.NP_OFFSET);
			total ++;
			offset += size;		
		}		
		// Max 32K in chain
		buf.putShort(pos, (short) total);
	}

	/**
	 * Gets the bucket list size.
	 *
	 * @param ptr the ptr
	 * @return the bucket list size
	 */
	private int getBucketListSize(long ptr) {
 
		// 2 bytes or total records
		int size = 2;
		while(ptr != 0){
			ptr = OffHeapCache.getRealAddress(ptr);
			size += IOUtils.getRecordSize(ptr/*+OffHeapCache.OFFSET*/) +OffHeapCache.OFFSET+8;
			ptr = IOUtils.getLong(ptr, OffHeapCache.NP_OFFSET);
		}
		return size;
	}

}

