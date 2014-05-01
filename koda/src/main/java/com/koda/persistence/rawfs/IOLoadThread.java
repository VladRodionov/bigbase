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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.google.common.io.LittleEndianDataInputStream;
import com.koda.config.ExtCacheConfiguration;
import com.koda.io.Worker;
import com.koda.persistence.ProgressListener;

// TODO: Auto-generated Javadoc
/**
 * The Class IOLoadThread.
 */
public class IOLoadThread extends Worker<ByteBuffer> {

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(IOLoadThread.class);	
	
	/** The file. */
	File file;
	
	/** The file channel. */
	FileChannel fileChannel;
	
	/** The fis. */
	FileInputStream fis;
	
	/** The dis. */
	DataInput dis;
	
	/** The progress listener. */
	ProgressListener pl;
	
	/** data root. */
	String dataRoot;
	
	/** store name. */
	String storeName;
	
	/** Byte buffer. */
	ByteBuffer buffer;
	
	/** OffHeapCache. */
	
	//OffHeapCache cache;
	private ExtCacheConfiguration cfg;
	
	/** The total read. */
	long totalRead = 0;
	
	/**
	 * Instantiates a new IO load thread.
	 *
	 * @param dataRoot the data root
	 * @param storeName the store name
	 * @param from the from
	 * @param to the to
	 * @param pl the pl
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public IOLoadThread(String dataRoot, String storeName, BlockingQueue<ByteBuffer> from,
			BlockingQueue<ByteBuffer> to, ProgressListener pl) throws IOException {
		super("IOLoadThread", from, to);
		this.pl = pl;
		this.dataRoot = dataRoot;
		this.storeName = storeName;
		init();
	}

	/**
	 * Gets the cache.
	 *
	 * @return the cache
	 */
//	public static OffHeapCache getCache()
//	{
//		return cache;
//	}
	
	public ExtCacheConfiguration getExtCacheConfiguration()
	{
	  return cfg;
	}
	
	/**
	 * Inits the.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void init() throws IOException {
		
		buffer = ByteBuffer.allocateDirect(4*4096);
		buffer.order(ByteOrder.nativeOrder());
		
		if(pl != null) pl.started();
		
		file = getLatestStoreFile();
		if(file == null) {
			LOG.warn("no saved caches found in :"+
					dataRoot+File.separator+" for cache ["+storeName+"]");
			//TODO
			if( pl != null) pl.finished();
			return;
		}
		fis = new FileInputStream(file);
		fileChannel = fis.getChannel();
		
		ByteOrder nativeByteOrder = ByteOrder.nativeOrder();
		boolean littleEndian = nativeByteOrder == ByteOrder.LITTLE_ENDIAN;
		if(littleEndian){
			// Use Guava
			dis = new LittleEndianDataInputStream(fis);
		} else{
			// Use standard JDK java.io
			dis = new DataInputStream(fis);
		}

		int cfgSize = dis.readInt();
		buffer.clear();
		buffer.limit(cfgSize);
		int total = 0;
		// Read all bytes from channel till 'cfgSize' limit
		while ((total += fileChannel.read(buffer)) < cfgSize);
			
		buffer.flip();
	  cfg = ExtCacheConfiguration.read(buffer, null);
		
		// Create cache instance first to make sure
		// that we set System property COMPRESSION_THRESHOLD
//		try{
//			cache = new OffHeapCache(cfg);
//		}catch(Exception e){
//			throw new IOException(e);
//		}
	}

	
	/**
	 * Gets the latest store file.
	 *
	 * @return the latest store file
	 */
	private final File getLatestStoreFile() {		
		String   lastSaved = dataRoot+File.separator+storeName+RawFSStore.FILE_EXT;
		File lastSavedFile = new File(lastSaved);
		String lastSnapshot = dataRoot+File.separator+RawFSStore.SNAPSHOTS +File.separator+ storeName+RawFSStore.FILE_EXT;
		File lastSnapshotFile = new File(lastSnapshot);
		long lastModified, lastSnapshotModified;
		
		File returnFile = null;
		
		boolean savedExists = lastSavedFile.exists();
		boolean snapExists = lastSnapshotFile.exists();
		if(! savedExists && !snapExists) {
			// no saved files and snapshots
			LOG.warn("[RawFSStore] no saved files found");

		} else if (!snapExists){
			// no snapshots
			returnFile = lastSavedFile;
		} else if (!savedExists){
			// no saved
			returnFile= lastSnapshotFile;
		} else{
		
			// Both exist - check last modification time
			lastModified = lastSavedFile.lastModified();
			lastSnapshotModified = lastSnapshotFile.lastModified();
			if(lastModified > lastSnapshotModified){
				returnFile = lastSavedFile;
			} else{
				returnFile = lastSnapshotFile;
			}
		}
		if(returnFile != null){
			LOG.info("[RawFSStore] Loading from : "+returnFile.getAbsolutePath());
		}
		return returnFile;
	}

	
	/* (non-Javadoc)
	 * @see com.koda.io.Worker#finish()
	 */
	@Override
	protected void finish() {
		try{
			fileChannel.close();
			if(pl != null) pl.finished();
		}catch(Exception e){
			LOG.error(e);
			if(pl != null) pl.error(e, true);
		}
		
	}

	/* (non-Javadoc)
	 * @see com.koda.io.Worker#process(java.lang.Object)
	 */
	@Override
	protected boolean process(ByteBuffer data) throws IOException {
		long fileLength = fileChannel.size();
		
		int blobSize = dis.readInt();
		//LOG.info(" file len ="+fileLength+" file offset ="+fileChannel.position()+" BLOB size="+blobSize+" BB capacity="+data.capacity());
		data.clear();
		data.limit(blobSize);
		int total = 0;
		// Read all bytes from channel till 'cfgSize' limit
		while ((total += fileChannel.read(data)) < blobSize){
		    //LOG.info("total ="+total);
		}
		
		totalRead += 4 + blobSize;
		data.flip();
		if(pl != null) pl.progress(totalRead, fileLength);
		//LOG.info("position="+fileChannel.position()+" length="+fileLength);
		if(fileChannel.position() == fileLength) return true;				
		return false;
	}

	

	
}
