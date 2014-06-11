/*******************************************************************************
* Copyright (c) 2013, 2014 Vladimir Rodionov. All Rights Reserved
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
package com.koda.integ.hbase.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.koda.cache.OffHeapCache;
import com.koda.integ.hbase.util.ConfigHelper;
import com.koda.util.Utils;
/**
 * This class implements file-based L3 cache with multiple cache directories. 
 * It supports up to 127 cache directories (it can do 256, but do we really need this?).
 * 
 */
public class FileExtMultiStorage implements ExtStorage {
	  /** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(FileExtMultiStorage.class);
	private FileExtStorage[] storages;
	
	/** The max storage size. */
	private long maxStorageSize ;	
	
	@Override
	public void config(Configuration cfg, OffHeapCache cache)
			throws IOException 
	{
		String value = cfg.get(FileExtStorage.FILE_STORAGE_BASE_DIR);
		if( value == null){
			throw new IOException("[FileExtMultiStorage] Base directory not specified.");
		}

		String[] cacheDirs = value.split(",");
		
		value = cfg.get(FileExtStorage.FILE_STORAGE_MAX_SIZE);
		if( value == null){
			throw new IOException("[FileExtMultiStorage] Maximum storage size not specified.");
		} else{
			maxStorageSize = Long.parseLong(value);
			LOG.info("[FileExtMultiStorage] Maximum storage size: "+maxStorageSize);
		}
		
		storages = new FileExtStorage[cacheDirs.length];
		long maxStorSize = this.maxStorageSize / storages.length;
		for(int i = 0; i < cacheDirs.length; i++){
			Configuration config = ConfigHelper.copy(cfg);
			config.set(FileExtStorage.FILE_STORAGE_BASE_DIR, cacheDirs[i]);
			config.set(FileExtStorage.FILE_STORAGE_MAX_SIZE, Long.toString(maxStorSize));
			storages[i] = new FileExtStorage();
			storages[i].config(config, cache);
		}

	}

	@Override
	public StorageHandle storeData(ByteBuffer buf) {
		int numStorages = storages.length;
		// Key length (16)
		int length = buf.getInt(4);
		int hash = Utils.hash(buf, 8, length, 0);
		int storageId = Math.abs(hash) % numStorages;
		FileStorageHandle handle = (FileStorageHandle) storages[storageId].storeData(buf);
		addCacheRootId(handle, storageId);
		return handle;
	}
	

	@Override
	public List<StorageHandle> storeDataBatch(ByteBuffer buf) {
		throw new RuntimeException("not implemented");
	}

	@Override
	public StorageHandle getData(StorageHandle storeHandle, ByteBuffer buf) {
		int id = getCacheRootId((FileStorageHandle) storeHandle);
		return addCacheRootId((FileStorageHandle)storages[id].getData(clearHandle((FileStorageHandle)storeHandle), buf), id);
	}

	@Override
	public void close() throws IOException {
		LOG.info("Closing down L3 cache storage");
		long start = System.currentTimeMillis();
		for(FileExtStorage storage: storages)
		{
			storage.close();
		}
		long end = System.currentTimeMillis();
		LOG.info("Completed in "+(end-start)+"ms");		

	}

	@Override
	public void flush() throws IOException {
		
		LOG.info("Flushing down L3 cache storage");
		long start = System.currentTimeMillis();
		for(FileExtStorage storage: storages)
		{
			storage.flush();
		}
		long end = System.currentTimeMillis();
		LOG.info("Completed in "+(end-start)+"ms");
	}

	@Override
	public long size() {
		long size = 0;
		for(FileExtStorage storage: storages)
		{
			size += storage.size();
		}
		return size;
	}

	@Override
	public void shutdown(boolean isPersistent) throws IOException {
		LOG.info("Shutting down L3 cache storage");
		long start = System.currentTimeMillis();
		for(FileExtStorage storage: storages)
		{
			storage.shutdown(isPersistent);
		}
		long end = System.currentTimeMillis();
		LOG.info("Completed in "+(end-start)+"ms");

	}

	@Override
	public long getMaxStorageSize() {
		return maxStorageSize;
	}

	@Override
	public StorageHandle newStorageHandle() {
		return new FileStorageHandle();
	}

	@Override
	public boolean isValid(StorageHandle h) {
		FileStorageHandle hh = ((FileStorageHandle) h).copy();
		int storageId = getCacheRootId((FileStorageHandle)hh);
		hh = clearHandle(hh);
		return storages[storageId].isValid(hh);
	}
	
	public long getFlushInterval()
	{
		if(storages[0] != null) return storages[0].getFlushInterval();
		else return -1;
	}

	public long getCurrentStorageSize()
	{
		long size = 0;
		for(FileExtStorage stor : storages)
		{
			size += stor.getCurrentStorageSize();
		}
		return size;
	}
	
	public int getMinId()
	{
		int min = Integer.MAX_VALUE;
		for(FileExtStorage stor: storages)
		{
			int v = stor.getMinId().get();
			if( v < min) min = v;
		}
		return min;
	}
	
	public int getMaxId()
	{
		int max = Integer.MIN_VALUE;
		for(FileExtStorage stor: storages)
		{
			int v = stor.getMaxId().get();
			if( v > max) max = v;
		}
		return max;
	}
	
	private final FileStorageHandle addCacheRootId(FileStorageHandle h, int id)
	{
		h.id = id << 24 | h.id; 
		return h;
	}
	
	private final int getCacheRootId(FileStorageHandle h)
	{
		return h.id >>> 24;
	}
	
	private final FileStorageHandle clearHandle(FileStorageHandle h)
	{
		h.id  = h.id & 0xffffff;
		return h;
	}

	public String getFilePath(FileStorageHandle h) {
		int id = getCacheRootId(h);
		return storages[id].getFilePath(h.getId() & 0xffffff);
	}
	
}
