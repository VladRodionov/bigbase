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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.koda.cache.OffHeapCache;
import com.koda.config.CacheConfiguration;
import com.koda.config.DiskStoreConfiguration;
import com.koda.config.ExtCacheConfiguration;
import com.koda.io.Worker;
import com.koda.persistence.ProgressListener;

// TODO: Auto-generated Javadoc
/**
 * The Class IOSaveThread.
 */
public class IOSaveThread extends Worker<ByteBuffer> {

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(IOSaveThread.class);
	
	/** The file channel. */
	private FileChannel fileChannel;
	
	/** The fos. */
	private FileOutputStream fos;
	
	/** The temp file. */
	private File tempFile;
	
	/** The file. */
	private File file;
	
	/** The mem store. */
	private OffHeapCache memStore;
	
	/** The data root. */
	private String dataRoot;
	
	/** The store name. */
	private String storeName;
	
	/** The buffer. */
	private ByteBuffer buffer;
	
	/** The pl. */
	private ProgressListener pl;
	
	/** The total saved. */
	private long totalSaved ;
	
	/** The total to save. */
	private long totalToSave;
	
	/**
	 * Instantiates a new iO save thread.
	 *
	 * @param cache the cache
	 * @param dataRoot the data root
	 * @param from the from
	 * @param to the to
	 * @param pl the pl
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public IOSaveThread( OffHeapCache cache, String dataRoot, BlockingQueue<ByteBuffer> from,
			BlockingQueue<ByteBuffer> to, ProgressListener pl) throws IOException {
		super("IOSaveThread", from, to);
		this.memStore = cache;
		this.dataRoot = dataRoot;
		this.pl = pl;
		init();
	}

	
	
	/**
	 * Inits the.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void init() throws IOException {
		// 16K is enough to serialize all configuration
		LOG.info(Thread.currentThread().getName()+" init() started");
		buffer = ByteBuffer.allocateDirect(4*4096);
		buffer.order(ByteOrder.nativeOrder());
		
		this.totalToSave = memStore.getTotalAllocatedMemorySize();
		if(pl != null) pl.started();				
		
		CacheConfiguration cconfig = memStore.getCacheConfiguration();
		DiskStoreConfiguration cfg = (DiskStoreConfiguration) cconfig.getDiskStoreConfiguration();
		if(!(cfg instanceof RawFSConfiguration)){
			throw new IOException("incompatible configuration object: "+cfg.getClass().getName());
		}
		RawFSConfiguration config = (RawFSConfiguration) cfg;

		this.storeName = config.getStoreName();
		if(this.storeName == null){
			this.storeName = cconfig.getQualifiedName();
		}
		this.file = getFileForStore();
		this.tempFile = getTempFileForStore();
		// Write configuration
		fos = new FileOutputStream(tempFile);

		fileChannel = fos.getChannel();
		fileChannel.truncate(0);
		buffer.clear();
		buffer.position(4);
		ExtCacheConfiguration.write(buffer, memStore.getExtCacheConfiguration(), null);						
		// buffer position after write?
		buffer.putInt(0, buffer.position() -4);
        buffer.flip();
        //TODO verify that FileChannel writes ALL data
        fileChannel.write(buffer);
        LOG.info(Thread.currentThread().getName()+" init() finished");
	}

	/**
	 * Gets the temp file for store.
	 *
	 * @return the temp file for store
	 */
	private File getTempFileForStore() {
		String   current = dataRoot+File.separator+storeName+RawFSStore.FILE_EXT+".current";
		File currentForSave = new File(current);
		return currentForSave;
	}

	/**
	 * Gets the file for store.
	 *
	 * @return the file for store
	 */
	private File getFileForStore() {
		String   current = dataRoot+File.separator+storeName+RawFSStore.FILE_EXT;
		File currentForSave = new File(current);
		return currentForSave;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.io.Worker#finish()
	 */
	@Override
	protected void finish() {
		LOG.info(Thread.currentThread().getName()+"# Finish");
		try {
			fileChannel.force(true);
			fileChannel.close();
			tempFile.renameTo(file);
			if(pl != null) pl.finished();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error(e);
			if(pl != null) pl.error(e, true);
		}
		LOG.info(Thread.currentThread().getName()+"# Finish completed");
		
	}

	/* (non-Javadoc)
	 * @see com.koda.io.Worker#process(java.lang.Object)
	 */
	@Override
	protected boolean process(ByteBuffer data) throws IOException {
		try{
			//LOG.info(Thread.currentThread().getName()+" process pos="+data.position()+" limit="+data.limit());
			totalSaved += data.limit();			
			//TODO java.nio.channels.ClosedByInterruptException
			fileChannel.write(data);
			if(pl != null) pl.progress(totalSaved, totalToSave);
		}catch(Exception e){
			if(pl != null) pl.error(e, true);
			throw new IOException(e);
		}
		return false;
	}

}
