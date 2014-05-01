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

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.koda.NativeMemoryException;
import com.koda.integ.hbase.blockcache.OffHeapBlockCacheOld;
import com.koda.integ.hbase.storage.ExtStorageManager;
import com.koda.integ.hbase.storage.FileExtStorage;
import com.koda.integ.hbase.storage.FileStorageHandle;
import com.koda.integ.hbase.storage.StorageHandle;

// TODO: Auto-generated Javadoc
/**
 * The Class FileStorageBasicTest.
 */
public class FileStorageBasicBatchTest extends FileStorageBaseTest{

	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(FileStorageBasicBatchTest.class);
	
	/** The m. */
	static int M = 10000;
	
	/** The to read. */
	static int toRead = 10000;
	/**
	 * Test put get.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testPutGet() throws NativeMemoryException, IOException
	{
		LOG.info("Test put-get batch started.");
		int size = 32987;
		int n = 10;
		
		byte[][] blocks = TestUtils.createByteArrays(size, n);
		putValues(blocks);
		
		List<StorageHandle> handles = storage.storeDataBatch(buffer);

		assertEquals(n, handles.size());
		
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		LOG.info("Waiting for flush complete - Done.");		
		buffer.clear();
		
		for(int i=0; i < n; i++){
			FileStorageHandle handle = (FileStorageHandle) storage.getData( handles.get(i), buffer);		
			assertTrue(handle.equals(handles.get(i)));		
			byte[] buf = getValue();	
			int newSize = buf.length;
			assertEquals(size, newSize);		
			int ss = Bytes.toInt(buf, 0);		
			assertEquals(size, ss);
		}
		
		LOG.info("Test put-get batch finished.");
	}
	
	/**
	 * Test multi put get.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testMultiPutGet() throws NativeMemoryException, IOException
	{
		LOG.info("Test multi put-get in a batch mode started.");
		//int M = 1000;
		int baseSize = 10000;
		int batchSize = 5;
		LOG.info("Inserting  "+M+" batches. Batch size: "+batchSize);

		Random r = new Random();
		long start = System.currentTimeMillis();
		long written = 0;
		for(int i=0; i <=M ; i++){
			int size = baseSize + r.nextInt(baseSize/10);
			byte[][] blocks = TestUtils.createByteArrays(size, batchSize);

			written+= batchSize *(size +4);
			//LOG.info("written="+written);
			
			putValues(blocks);
			List<StorageHandle> handles = storage.storeDataBatch(buffer);			
			assertEquals(batchSize, handles.size());
			for(int j=0; j < batchSize; j++){
				String skey = key + (i*batchSize + j);
				cache.put(skey, handles.get(j));		
			}
						
		}
		
		LOG.info("Storing "+(M* batchSize)+" objects took "+(System.currentTimeMillis() - start)+"ms ");
		LOG.info("Storage size="+storage.getCurrentStorageSize()+" minId="+storage.getMinId()+" maxId="+storage.getMaxId());
		
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		
		LOG.info("Waiting for flush complete - Done.");		
		buffer.clear();		
		//int toRead= 10000;
		LOG.info("Reading random "+ toRead+" objects ");
		start = System.currentTimeMillis();
		for(int i=0; i < toRead; i++){
			String skey = key + (r.nextInt(M* batchSize-1) + 1);
			FileStorageHandle handle = (FileStorageHandle) cache.get(skey);
			//LOG.info("key="+skey);
			//LOG.info("i="+i+" handle="+handle.toString());
			storage.getData(handle, buffer);
			byte[] block = getValue();
			//LOG.info("i="+i+" block size "+block.length+" handle="+handle.toString());
			
			assertTrue(block != null);
			assertEquals(block.length, Bytes.toInt(block, 0));
		}
		
		LOG.info("Reading "+toRead+" objects took "+(System.currentTimeMillis() -start)+"ms");
		LOG.info("Test multi put-get in a batch mode finished.");
	}
	
	/**
	 * Test multi put get.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testMultiPutGetWithReplacement() throws NativeMemoryException, IOException
	{
		LOG.info("Test multi put-get in a batch mode started (replacement).");
		//int M = 1000;
		int baseSize = 10000;
		int batchSize = 5;
		LOG.info("Inserting another "+M+" batches. Batch size: "+batchSize);

		Random r = new Random();
		long start = System.currentTimeMillis();
		long written = 0;
		for(int i= M; i <= 2*M ; i++){
			int size = baseSize + r.nextInt(baseSize/10);
			byte[][] blocks = TestUtils.createByteArrays(size, batchSize);

			written+= batchSize *(size +4);
			
			putValues(blocks);
			List<StorageHandle> handles = storage.storeDataBatch(buffer);			
			assertEquals(batchSize, handles.size());
			for(int j=0; j < batchSize; j++){
				String skey = key + (i*batchSize + j);
				cache.put(skey, handles.get(j));		
			}
						
		}
				
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		LOG.info("Storing "+(M* batchSize)+" objects took "+(System.currentTimeMillis() - start)+"ms ");
		LOG.info("Storage size="+storage.getCurrentStorageSize()+" minId="+storage.getMinId()+" maxId="+storage.getMaxId());
		
		LOG.info("Waiting for flush complete - Done.");	
		
		buffer.clear();		
		//int toRead= 10000;
		LOG.info("Reading random "+ toRead+" objects ");
		start = System.currentTimeMillis();
		int failed = 0;
		for(int i=0; i < toRead; i++){
			String skey = key + ((r.nextInt(M* batchSize-1) + 1) + M*batchSize);
			FileStorageHandle handle = (FileStorageHandle) cache.get(skey);
			//LOG.info("key="+skey);
			//LOG.info("i="+i+" handle="+handle.toString());
			storage.getData(handle, buffer);
			byte[] block = getValue();
			//LOG.info("i="+i+" block size "+block.length+" handle="+handle.toString());
			
			assertTrue(block != null);
			//assertEquals(block.length, Bytes.toInt(block, 0));
			if(block.length != Bytes.toInt(block,0)){
				failed ++;
			}
		}
		
		LOG.info("Reading "+toRead+" objects took "+(System.currentTimeMillis() -start)+"ms. Failed ="+failed);
		LOG.info("Test multi put-get in a batch mode finished (replacement).");
	}
	
	/**
	 * Test close storage.
	 */
	public void testCloseStorage()
	{
		LOG.info("Test close storage started");
		try{
			storage.close();
		} catch(IOException e){
			assertTrue(false);
		}
		LOG.info("Test close storage finished.");
	}
	
	/**
	 * Test new storage write read.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testNewStorageWriteRead() throws NativeMemoryException, IOException
	{
		LOG.info("Test new storage multi put-get with replacement in a batch mode started.");
						
		Configuration config = new Configuration();
		config.set(FileExtStorage.FILE_STORAGE_BASE_DIR, baseDir);
		config.set(FileExtStorage.FILE_STORAGE_MAX_SIZE, "1000000000");
		config.set(OffHeapBlockCacheOld.EXT_STORAGE_IMPL, "com.koda.integ.hbase.storage.FileExtStorage");
		// 10MB file size limit
		config.set(FileExtStorage.FILE_STORAGE_FILE_SIZE_LIMIT,"10000000");
		// 2MB buffer size 
		config.set(FileExtStorage.FILE_STORAGE_BUFFER_SIZE,"2000000");
				
		storage = (FileExtStorage) ExtStorageManager.getInstance().getStorage(config, cache);
		LOG.info("New storage storage opened. Size="+storage.getCurrentStorageSize());		
				
		int baseSize = 10000;
		int batchSize = 5;
		LOG.info("Inserting another "+M+" batches. Batch size: "+batchSize);

		Random r = new Random();
		long start = System.currentTimeMillis();
		long written = 0;
		for(int i= 2*M; i <= 3*M ; i++){
			int size = baseSize + r.nextInt(baseSize/10);
			byte[][] blocks = TestUtils.createByteArrays(size, batchSize);

			written+= batchSize *(size +4);
			
			putValues(blocks);
			List<StorageHandle> handles = storage.storeDataBatch(buffer);			
			assertEquals(batchSize, handles.size());
			for(int j=0; j < batchSize; j++){
				String skey = key + (i*batchSize + j);
				cache.put(skey, handles.get(j));		
			}
						
		}
				
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		LOG.info("Storing "+(M* batchSize)+" objects took "+(System.currentTimeMillis() - start)+"ms ");
		LOG.info("Storage size="+storage.getCurrentStorageSize()+" minId="+storage.getMinId()+" maxId="+storage.getMaxId());
		
		LOG.info("Waiting for flush complete - Done.");	
		
		buffer.clear();		
		//int toRead= 10000;
		LOG.info("Reading random "+ toRead+" objects ");
		start = System.currentTimeMillis();
		int failed = 0, nulls =0;
		for(int i=0; i < toRead; i++){
			String skey = key + ((r.nextInt(M* batchSize-1) + 1) + 2*M*batchSize);
			FileStorageHandle handle = (FileStorageHandle) cache.get(skey);
			//LOG.info("key="+skey);
			//LOG.info("i="+i+" handle="+handle.toString());
			storage.getData(handle, buffer);
			byte[] block = getValue();
			//LOG.info("i="+i+" block size "+block.length+" handle="+handle.toString());
			
			//assertTrue(block != null);
			if(block == null) nulls ++;
			//assertEquals(block.length, Bytes.toInt(block, 0));
			if(block.length != Bytes.toInt(block,0)){
				failed ++;
			}
		}		

		
		LOG.info("Reading "+toRead+" objects took "+(System.currentTimeMillis() -start)+"ms. Total nulls="+nulls+" failed="+failed);
		LOG.info("Test new storage multi put-get with replacement in a batch mode finished.");
	}
	
}
