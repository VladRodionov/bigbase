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
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.koda.cache.CacheManager;
import com.koda.cache.OffHeapCache;
import com.koda.compression.CodecType;
import com.koda.config.CacheConfiguration;
import com.koda.integ.hbase.blockcache.OffHeapBlockCache;
import com.koda.integ.hbase.storage.ExtStorageManager;
import com.koda.integ.hbase.storage.FileExtMultiStorage;
import com.koda.integ.hbase.storage.FileExtStorage;
import com.koda.integ.hbase.util.StorageHandleSerializer;

// TODO: Auto-generated Javadoc
/**
 * The Class FileStorageBasicTest.
 */
public class FileMultiStorageBaseTest extends TestCase{

	/** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(FileMultiStorageBaseTest.class);
	
	/** The init done. */
	static boolean initDone = false;
	
	/** The base dir. */
	static String baseDir = "/tmp/cache1,/tmp/cache2";
	
	/** The cache. */
	static OffHeapCache cache ;
	
	/** The storage. */
	static FileExtMultiStorage storage;
	
	/** The key. */
	static String key = "key-";
	
	/** The buffer. */
	static ByteBuffer buffer = ByteBuffer.allocateDirect(4*1024*1024);
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp() throws Exception {
		if(initDone) return;

		// Remove old data
		deleteData();
		
		CacheManager manager = CacheManager.getInstance();	
		CacheConfiguration cfg = new CacheConfiguration();
		// Max memory = 100MB
		cfg.setMaxMemory(100000000);
		// Bucket number 
		cfg.setBucketNumber(2000000);
		cfg.setCodecType(CodecType.LZ4);
		cfg.setHistogramEnabled(true);
		cfg.setHistogramUpdateInterval(500);
		cache = manager.createCache(cfg);
		// SerDe
		StorageHandleSerializer serde2 = new StorageHandleSerializer();
		cache.getSerDe().registerSerializer(serde2);
		
		Configuration config = new Configuration();
		config.set(FileExtStorage.FILE_STORAGE_BASE_DIR, baseDir);
		config.set(FileExtStorage.FILE_STORAGE_MAX_SIZE, "1000000000");
		config.set(OffHeapBlockCache.BLOCK_CACHE_EXT_STORAGE_IMPL, "com.koda.integ.hbase.storage.FileExtMultiStorage");
		// 10MB file size limit
		config.set(FileExtStorage.FILE_STORAGE_FILE_SIZE_LIMIT,"10000000");
		// 2MB buffer size 
		config.set(FileExtStorage.FILE_STORAGE_BUFFER_SIZE,"2000000");
		
		checkDir();
		
		storage = (FileExtMultiStorage) ExtStorageManager.getInstance().getStorage(config, cache);		
		initDone = true;
	}

	private void deleteData() {
		String[] dirs = baseDir.split(",");
		for(String sdir: dirs){
			File dir = new File(sdir);
			if (dir.exists()) {
				File[] files = dir.listFiles();
				for (File f : files) {					
					f.delete();
					LOG.info("Deleted "+f.getAbsolutePath());
				}
			}
		}
	}

  /**
	 * Check dir.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void checkDir() throws IOException
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
	protected byte[] getValue()
	{
		int size =buffer.getInt(0);
		if( size <=0 ) return null;
		byte[] bytes = new byte[size];
		buffer.limit(size + 4);
		buffer.position(4);
		buffer.get(bytes);
		return bytes;
	}
	
	
	/**
	 * Gets the value from key value.
	 *
	 * @return the value from key value
	 */
	protected byte[] getValueFromKeyValue()
	{
		int size =buffer.getInt(0);
		if( size <=0 ) return null;
		int keySize = buffer.getInt(4);
		byte[] bytes = new byte[size - keySize - 8];
		buffer.limit(size );
		buffer.position(8 + keySize);
		buffer.get(bytes);
		return bytes;
	}
	
	/**
	 * Gets the key from key value.
	 *
	 * @return the key from key value
	 */
	protected byte[] getKeyFromKeyValue()
	{
		int keySize = buffer.getInt(4);
		byte[] bytes = new byte[keySize];
		buffer.limit( keySize + 8 );
		buffer.position(8);
		buffer.get(bytes);
		return bytes;
	}
	/**
	 * Put value.
	 *
	 * @param block the block
	 */
	protected void putValue(byte[] block)
	{
		buffer.clear();
		buffer.putInt(block.length);
		buffer.put(block);
		buffer.flip();
	}

	/**
	 * Put key - value.
	 *
	 * @param key the key
	 * @param block the block
	 */
	protected void putKeyValue(byte[] key, byte[] block)
	{
		buffer.clear();
		buffer.putInt(key.length + block.length + 8);
		buffer.putInt(key.length);
		buffer.put(key);
		buffer.put(block);
		buffer.flip();
	}
	
	/**
	 * Put values.
	 *
	 * @param blocks the blocks
	 */
	protected void putValues(byte[][] blocks)
	{
		buffer.clear();
		buffer.position(4);
		for(int i = 0; i < blocks.length; i++){
			buffer.putInt(blocks[i].length);
			buffer.put(blocks[i]);
		}
		buffer.putInt(0, buffer.position() - 4);
		buffer.flip();
	}
	
	/**
	 * Put key values.
	 *
	 * @param keys the keys
	 * @param blocks the blocks
	 */
	protected void putKeyValues(byte[][] keys, byte[][] blocks)
	{
		buffer.clear();
		buffer.position(4);
		for(int i = 0; i < blocks.length; i++){
			buffer.putInt(keys[i].length + blocks[i].length + 8);
			buffer.putInt(keys[i].length);
			buffer.put(keys[i]);
			buffer.put(blocks[i]);
		}
		buffer.putInt(0, buffer.position() - 4);
		buffer.flip();
	}
	
}
