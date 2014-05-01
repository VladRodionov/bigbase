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
 * The Class LoadThread.
 */
public class LoadThread extends Worker<ByteBuffer> {
	private final static Logger LOG = Logger.getLogger(LoadThread.class);
	/** The mem store. */
	OffHeapCache memStore;
	
	/** The codec. */
	Codec codec;
	
	/** The temp buffer. */
	ByteBuffer tempBuffer;
	
	/**
	 * Instantiates a new load thread.
	 *
	 * @param cache the cache
	 * @param from the from
	 * @param to the to
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public LoadThread(OffHeapCache cache, BlockingQueue<ByteBuffer> from,
			BlockingQueue<ByteBuffer> to) throws IOException{
		super("LoadThread", from, to);
		this.memStore = cache;
		init();
	}

	/**
	 * Inits the.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void init() throws IOException
	{
		DiskStoreConfiguration cfg =memStore.getCacheConfiguration().getDiskStoreConfiguration();
		int bufferSize = ((RawFSConfiguration) cfg).getRWBufferSize(); 
		tempBuffer = ByteBuffer.allocateDirect(bufferSize);
		tempBuffer.order(ByteOrder.nativeOrder());
		

        codec = memStore.getCacheConfiguration().getDiskStoreConfiguration().
        											getDbCompressionType().getCodec();
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
	protected boolean process(ByteBuffer data) throws IOException {
		
		if(codec != null){
			// Decompress data
			tempBuffer.clear();
			codec.decompress(data, tempBuffer);
			loadDataFromTo(tempBuffer, memStore);
		} else{				
			loadDataFromTo(data, memStore);
		}
		return false;
	}

	/**
	 * Load data from to.
	 *
	 * @param buf the buf
	 * @param offheap the offheap
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void loadDataFromTo(ByteBuffer buf, OffHeapCache offheap) throws IOException {
		
		
		while(buf.remaining() > 0){
			try{
			  //LOG.info(buf.remaining());
				loadChainedData(buf, offheap);
			}catch(Exception e){
			  e.printStackTrace();
				throw new IOException(e);
			}
		}
		//LOG.info(buf.remaining());
		
	}

	/**
	 * Load chained data.
	 * TODO: extra large buffers (> 2G)
	 * 
	 * @param buf the buf
	 * @param cache the cache
	 * @throws NativeMemoryException the native memory exception
	 */
	private void loadChainedData(ByteBuffer buf, OffHeapCache cache) throws NativeMemoryException {
		long bufptr = cache.getBufferAddress();

		// Get initial bucket index in offheapcache buffer
		long index = buf.getLong();
		//LOG.info("Index ="+index);
		// Convert index to real address
		long address = bufptr ;//+ index * 8;
		// get number of records in this bucket
		int numRecords = buf.getShort();// Max 32K record in one bucket
		int recordsLoaded = 0;
		long ptr = 0;
		while(recordsLoaded++ < numRecords){
			int start = buf.position();
			// advance by OFFSET
			buf.position(start +OffHeapCache.OFFSET );
			// Size of a record = OFFSET+ keyLength + valueLength
			int size = OffHeapCache.OFFSET + buf.getInt() + buf.getInt() + 8;
			ptr = NativeMemory.malloc(size);
			// copy from buffer: position= current -OFFSET - 8;
			NativeMemory.memcpy(buf, start , size, ptr, 0);
			long nptr = ptr | 0x8000000000000000L;
			
//      IOUtils.putLong(address, 0, nptr);

			if(recordsLoaded == 1){
			  IOUtils.putLong(address, index * 8, nptr);
			} else{
	       IOUtils.putLong(address, OffHeapCache.NP_OFFSET, nptr);
			}
			//address = ptr + OffHeapCache.NP_OFFSET;			
			address = ptr;
			// advance buffer position to next record
			buf.position(start + size);
		}
		// set 0 as next ptr for last record
    //IOUtils.putLong(address, 0, 0);
		IOUtils.putLong(address, OffHeapCache.NP_OFFSET, 0);
		
	}

	
}
