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

// TODO: Auto-generated Javadoc
/**
 * The Class FileStorageBasicTest.
 */
public class FileStorageBasicTest extends FileStorageBaseTest{

	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(FileStorageBasicTest.class);
	

	
	/**
	 * Test put get.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testPutGet() throws NativeMemoryException, IOException
	{
		LOG.info("Test put-get started.");
		int size = 32987;
		byte[] block = TestUtils.createByteArray(size);
		String skey = key + 0;
		cache.put(skey, block);		
		byte[] value = (byte[]) cache.get(skey);
		assertTrue (value != null);
		assertEquals(size, value.length);
		assertEquals(size, Bytes.toInt(value, 0));

		putKeyValue(skey.getBytes(),block);
		
		FileStorageHandle handle = (FileStorageHandle)storage.storeData(buffer);
		assertEquals(0, handle.getId());
		assertEquals(0, handle.getOffset());
		//assertEquals(size, handle.getSize());
		
		cache.put(skey, handle);
		
		FileStorageHandle handle2 = (FileStorageHandle) cache.get(skey);
		
		assertEquals(handle, handle2);
		
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		LOG.info("Waiting for flush complete - Done.");		
		buffer.clear();
		
		FileStorageHandle handle3 = (FileStorageHandle) storage.getData( handle2, buffer);
		
		assertTrue(handle3.equals(handle2));
		
		byte[] buf = getValueFromKeyValue();	
		int newSize = buf.length;
		assertEquals(size, newSize);
		
		int ss = Bytes.toInt(buf, 0);
		
		assertEquals(size, ss);
		
		LOG.info("Test put-get finished.");
	}
	
	/**
	 * Test multi put get.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testMultiPutGet() throws NativeMemoryException, IOException
	{
		LOG.info("Test multi put-get started.");
		int M = 80000;
		LOG.info("Insert "+M+" objects. ");
		int baseSize = 10000;
		Random r = new Random();
		long start = System.currentTimeMillis();
		for(int i=1; i <=M ; i++){
			int size = baseSize + r.nextInt(baseSize/10);
			byte[] block = TestUtils.createByteArray(size);

			String skey = key + i;
			
			putKeyValue(skey.getBytes(), block);
			FileStorageHandle handle = (FileStorageHandle) storage.storeData(buffer);			
			
			cache.put(skey, handle);		
						
		}
		
		LOG.info("Storing "+M+" objects took "+(System.currentTimeMillis() - start)+"ms ");
		LOG.info("Storage size="+storage.getCurrentStorageSize()+" minId="+storage.getMinId()+" maxId="+storage.getMaxId());
		
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		LOG.info("Waiting for flush complete - Done.");		
		buffer.clear();		
		int toRead= 100000;
		LOG.info("Reading random "+ toRead+" objects ");
		start = System.currentTimeMillis();
		for(int i=0; i < toRead; i++){
			String skey = key + (r.nextInt(M-1) + 1);
			FileStorageHandle handle = (FileStorageHandle) cache.get(skey);
			//LOG.info("i="+i+" handle="+handle.toString());
			storage.getData(handle, buffer);
			byte[] block = getValueFromKeyValue();
			byte[] key   = getKeyFromKeyValue();
			//LOG.info("i="+i+" block size "+block.length+" handle="+handle.toString());
			assertTrue(key != null);
			assertEquals(skey,  new String(key));
			assertTrue(block != null);
			assertEquals(block.length, Bytes.toInt(block, 0));
		}
		
		LOG.info("Reading "+toRead+" objects took "+(System.currentTimeMillis() -start)+"ms");
		LOG.info("Test multi put-get finished.");
	}
	
	/**
	 * Test multi put get.
	 *
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void testMultiPutGetWithReplacement() throws NativeMemoryException, IOException
	{
		LOG.info("Test multi put-get with replacement started.");
		int M = 80000;
		LOG.info("Insert "+ M + " objects. ");
		int baseSize = 10000;
		Random r = new Random();
		long start = System.currentTimeMillis();
		for(int i=1; i <=M ; i++){
			int size = baseSize + r.nextInt(baseSize/10);
			byte[] block = TestUtils.createByteArray(size);

			String skey = key + (i + 80000);
			
			putKeyValue(skey.getBytes(), block);
			FileStorageHandle handle = (FileStorageHandle) storage.storeData(buffer);			
			
			cache.put(skey, handle);		
						
		}
		
		LOG.info("Storing "+M+" objects took "+(System.currentTimeMillis() - start)+"ms ");
		LOG.info("Storage size="+storage.getCurrentStorageSize()+" minId="+storage.getMinId()+" maxId="+storage.getMaxId());
		
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		LOG.info("Waiting for flush complete - Done.");		
		buffer.clear();		
		int toRead= 100000;
		LOG.info("Reading random "+ toRead+" objects ");
		start = System.currentTimeMillis();
		int nulls = 0;
		for(int i=0; i < toRead; i++){
			String skey = key + (r.nextInt(M-1) + 1 + 80000);
			FileStorageHandle handle = (FileStorageHandle) cache.get(skey);
			//LOG.info("i="+i+" handle="+handle.toString());
			storage.getData(handle, buffer);
			byte[] block = getValueFromKeyValue();
			//LOG.info("i="+i+" block size "+block.length+" handle="+handle.toString());
			
			//assertTrue(block != null);
			if( block != null){
				assertEquals(block.length, Bytes.toInt(block, 0));
			} else{
				nulls++;
			}
		}
		
		LOG.info("Reading "+toRead+" objects took "+(System.currentTimeMillis() -start)+"ms. Total nulls="+nulls);
		LOG.info("Test multi put-get with replacement finished.");
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
		LOG.info("Test new stoarge multi put-get with replacement started.");
						
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
				
		
		int M = 80000;
		LOG.info("Insert "+ M + " objects. ");
		int baseSize = 10000;
		Random r = new Random();
		long start = System.currentTimeMillis();
		for(int i=1; i <=M ; i++){
			int size = baseSize + r.nextInt(baseSize/10);
			byte[] block = TestUtils.createByteArray(size);

			String skey = key + (i + 80000 + 80000);
			
			putKeyValue(skey.getBytes(),block);
			FileStorageHandle handle = (FileStorageHandle) storage.storeData(buffer);			
			
			cache.put(skey, handle);		
						
		}
		
		LOG.info("Storing "+M+" objects took "+(System.currentTimeMillis() - start)+"ms ");
		LOG.info("Storage size="+storage.getCurrentStorageSize()+" minId="+storage.getMinId()+" maxId="+storage.getMaxId());
		
		long flushInterval = storage.getFlushInterval();
		LOG.info("Waiting for flush complete");
		try {
			Thread.sleep(flushInterval + 1000);
		} catch (InterruptedException e) {
			
		}
		LOG.info("Waiting for flush complete - Done.");		
		buffer.clear();		
		int toRead= 100000;
		LOG.info("Reading random "+ toRead+" objects ");
		start = System.currentTimeMillis();
		int nulls = 0;
		for(int i=0; i < toRead; i++){
			String skey = key + (r.nextInt(M-1) + 1 + 80000 + 80000);
			FileStorageHandle handle = (FileStorageHandle) cache.get(skey);
			//LOG.info("i="+i+" handle="+handle.toString());
			storage.getData(handle, buffer);
			byte[] block = getValueFromKeyValue();
			//LOG.info("i="+i+" block size "+block.length+" handle="+handle.toString());
			
			//assertTrue(block != null);
			if( block != null){
				assertEquals(block.length, Bytes.toInt(block, 0));
			} else{
				nulls++;
			}
		}
		
		LOG.info("Reading "+toRead+" objects took "+(System.currentTimeMillis() -start)+"ms. Total nulls="+nulls);
		LOG.info("Test new storage multi put-get with replacement finished.");
	}
	
}
