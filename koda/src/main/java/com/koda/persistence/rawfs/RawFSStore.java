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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.log4j.Logger;

import com.google.common.io.LittleEndianDataInputStream;
import com.koda.IOUtils;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.UnsupportedOperationException;
import com.koda.cache.OffHeapCache;
import com.koda.compression.Codec;
import com.koda.config.CacheConfiguration;
import com.koda.config.DiskStoreConfiguration;
import com.koda.config.ExtCacheConfiguration;
import com.koda.persistence.DiskStore;
import com.koda.persistence.DiskStoreException;
import com.koda.persistence.ProgressListener;


// TODO: Auto-generated Javadoc
/**
 * Raw FileSystem store. This store does not support
 * K-V operations, no evictions - only bulk load/store
 * and periodic snapshots. The disc cache size is always
 * 1:1 to in memory cache size.
 * 
 * TODO: 1.(+)compression
 *       2.(+) thread pool (for compression/decompression)
 *       3. move compression to base class
 *       (+)4. implement cancel 
 *       (+)5. 'ignoreExpired'
 *       6. BUG: when cache is loaded eviction list size is empty
 *       
 * TODO: additional testing : 
 *      1. Load from latest snapshot using loadFast
 *  
 *             
 * TODO: Large buffers (> 2G objects)
 *             
 * @author vrodionov
 *
 */

public class RawFSStore extends DiskStore {

	/** The Constant FILE_EXT. */
	public final static String FILE_EXT = ".dat";
	
	/** The Constant SNAPSHOTS. */
	public final static String SNAPSHOTS = ".snapshots";
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(RawFSStore.class);	
	/** The config. */
	private RawFSConfiguration config;
	
	/** The buffer. */
	private ByteBuffer buffer;	
	
	/** The temp buffer. */
	private ByteBuffer tempBuffer;
	
	/** The buffer size. */
	private int bufferSize;
	
	/** The disk store root. */
	private String[] diskStoreRoots;
	
	/** disk store name. */
	private String storeName;
	
	/** The file channel. */
	private FileChannel fileChannel;
	
	/** The cache. */
	private OffHeapCache cache;

	
	/** The snapshots activated. */
	private boolean snapshotsActivated = false;
	
	/** The snapshot thread. */
	private Thread snapshotThread;
	
	/**
	 * Instantiates a new raw fs store.
	 */
	public RawFSStore(){}
	
	/**
	 * Instantiates a new raw fs store.
	 *
	 * @param config the config
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public RawFSStore(CacheConfiguration config) throws IOException
	{
		this();
		init(config);
		
	}
	
	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#init(com.koda.config.DiskStoreConfiguration)
	 */
	protected void init(CacheConfiguration cconfig) throws IOException
	{
		DiskStoreConfiguration cfg = (DiskStoreConfiguration) cconfig.getDiskStoreConfiguration();
		if(!(cfg instanceof RawFSConfiguration)){
			throw new DiskStoreException("incompatible configuration object: "+cfg.getClass().getName());
		}
		RawFSConfiguration config = (RawFSConfiguration) cfg;
		this.config = config;
		this.bufferSize = config.getRWBufferSize();
		this.storeName = config.getStoreName();
		if(this.storeName == null){
			this.storeName = cconfig.getQualifiedName();
		}
		buffer = ByteBuffer.allocateDirect(bufferSize);
		buffer.order(ByteOrder.nativeOrder());
		
		// We use temp buffer only if data is compressed
		// on disk. 
		// TODO 
		tempBuffer = ByteBuffer.allocateDirect(bufferSize);
		tempBuffer.order(ByteOrder.nativeOrder());
		
		//TODO support for multiple disk store volumes
		this.diskStoreRoots = config.getDbDataStoreRoots();
		mkdirs();
	}
	
	/**
	 * Mkdirs.
	 */
	private void mkdirs() {
		for(String dir: diskStoreRoots){
			File root = new File(dir);
			if(!root.exists()){
				root.mkdirs();
			}
		}
		
		// Create snapshot dir in a first root
		
		File snapshotDir = new File(diskStoreRoots[0], SNAPSHOTS);
		if(!snapshotDir.exists()){
			snapshotDir.mkdirs();
		}
		
		
	}
	
	
	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#close()
	 */
	@Override
	public void close() throws IOException {
		if(! state.compareAndSet(State.IDLE, State.DISABLED))
		{
			throw new IOException("Can't close. Incompatible state");
		}
		if(fileChannel != null){
			fileChannel.force(true);
			fileChannel.close();
		}

		// We can't open store after closing
		
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#delete(java.nio.ByteBuffer)
	 */
	@Override
	public void delete(ByteBuffer key) throws IOException {
		throw new UnsupportedOperationException("delete");
		
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#flush()
	 */
	@Override
	public void flush() throws IOException {
		if(fileChannel != null){
			fileChannel.force(true);
		}
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#get(java.nio.ByteBuffer)
	 */
	@Override
	public void get(ByteBuffer key) throws IOException {
		throw new UnsupportedOperationException("get");
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#getConfig()
	 */
	@Override
	public DiskStoreConfiguration getConfig() {
		return config;
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#load(com.koda.persistence.ProgressListener)
	 */
	@Override
	public OffHeapCache load( ProgressListener pl)
			throws IOException {

		// Check that the store in an IDLE state
		if(!state.compareAndSet(State.IDLE, State.LOAD)){
			throw new IOException("Can't load. Incompatible state.");
		}
		
		FileInputStream fis = null;
		DataInput dis = null;
		ByteOrder nativeByteOrder = ByteOrder.nativeOrder();
		boolean littleEndian = nativeByteOrder == ByteOrder.LITTLE_ENDIAN;
		Codec codec = null;
		try {
			// Root directory for
			File f = getLatestStoreFile();
			// Nothing was found
			if(f == null) {
				LOG.warn("no saved caches found in :"+
						diskStoreRoots+File.separator+" for cache ["+storeName+"]");
				return null;
			}
			
			
			fis = new FileInputStream(f);
			if(littleEndian){
				// Use Guava
				dis = new LittleEndianDataInputStream(fis);
			} else{
				// Use standard JDK java.io
				dis = new DataInputStream(fis);
			}
			fileChannel = fis.getChannel();
			long fileLength = fileChannel.size();
			int cfgSize = dis.readInt();
			buffer.clear();
			buffer.limit(cfgSize);
			int total = 0;
			// Read all bytes from channel till 'cfgSize' limit
			while ((total += fileChannel.read(buffer)) < cfgSize);
				
			buffer.flip();
			ExtCacheConfiguration cfg = ExtCacheConfiguration
					.read(buffer, null);

			
			// Create cache instance first to make sure
			// that we set System property COMPRESSION_THRESHOLD
			cache = new OffHeapCache(cfg);
			
			// Compression codec used
			codec = cfg.getCacheConfig().
				getDiskStoreConfiguration().getDbCompressionType().getCodec();
			
			long totalRead = 4 + cfgSize;
			while (totalRead < fileLength) {
				int blobSize = dis.readInt();
				//LOG.info("\n\nBlob size="+blobSize+"\n\n");
				buffer.clear();
				buffer.limit(blobSize);
				total = 0;
				// Read all bytes from channel till 'cfgSize' limit
				while ((total += fileChannel.read(buffer)) < blobSize);
				//LOG.info("Total read ="+total);	
				buffer.flip();
				//LOG.info("[LOAD] buffer="+buffer.limit());
				if(codec != null){
					// Decompress data
					tempBuffer.clear();
					codec.decompress(buffer, tempBuffer);
					//tempBuffer.flip();
					//LOG.info("[LOAD] decompressed pos="+tempBuffer.position()+" limit="+tempBuffer.limit());
					loadDataFromTo(tempBuffer, cache);
				} else{				
					loadDataFromTo(buffer, cache);
				}
				totalRead += blobSize + 4;
				// check cancelled
				if(isCanceled()){					
					// Notify listener
					if(pl != null){ pl.canceled(); }											
					throw new RuntimeException("canceled");
				} else{
					if(pl != null){
						pl.progress(totalRead, fileLength);
					}
				}
			}
			
			if(pl != null) { pl.finished(); }
			
			return cache;
		} catch (Exception e) {
			
			if (pl != null){ pl.error(e, true);}			
			throw new IOException(e);
			
		} finally{
			if(isCanceled())
			{
				cancelRequested = false;
			}
			if(fileChannel != null && fileChannel.isOpen()){
				fileChannel.close();
			}
			//TODO
			state.compareAndSet(State.LOAD, State.IDLE);
		}		

	}

	/**
	 * Temp implementation.
	 *
	 * @param pl the pl
	 * @return the off heap cache
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public OffHeapCache loadFast(ProgressListener pl)
		throws IOException
	{
		
		boolean loadFromSnap = loadFromSnapshot();
		LOG.info("Loading from snapshot: "+loadFromSnap);
		long startTime = System.currentTimeMillis();
		
		//
		// We need to check what is the latest version of saved data
		//
		
		LOG.info("Loading data started");
		int numWorkers = config.getTotalWorkerThreads();
		int numIO = loadFromSnap? 1: config.getTotalIOThreads();
    if(numIO != diskStoreRoots.length) {
      numIO = diskStoreRoots.length;
    }
    
		int bufSize = config.getRWBufferSize();
		LOG.info("[load_fast] Workers    ="+numWorkers);
		LOG.info("[load_fast] IO threads ="+numIO);
		
		String[] loadRoots = diskStoreRoots;

		if(loadFromSnap) {
			loadRoots = new String[]{getSnapshotDir()};
		}
		
		ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
		for(int i=0; i < numWorkers; i++){
			ByteBuffer buf = ByteBuffer.allocateDirect(bufSize);
			buf.order(ByteOrder.nativeOrder());
			buffers.add(buf);
		}
		
		ArrayBlockingQueue<ByteBuffer> emptyBuffers = 
			new ArrayBlockingQueue<ByteBuffer>(numWorkers, false, buffers);
		ArrayBlockingQueue<ByteBuffer> filledBuffers = 
			new ArrayBlockingQueue<ByteBuffer>(numWorkers);
		
		// start IO threads

		IOLoadThread[] ioworkers = new IOLoadThread[numIO];
		OffHeapCache cache = null; 
		for(int i=0; i < numIO; i++)
		{
			// TODO Multiple IO threads support
			ioworkers[i] = new IOLoadThread(loadRoots[i], storeName,  emptyBuffers, filledBuffers, pl);
			if(i == 0){
			  try{
			    ExtCacheConfiguration cfg = ioworkers[i].getExtCacheConfiguration();
			    if(cfg != null) cache = new OffHeapCache(cfg);
			  } catch(Exception e){
			    throw new IOException (e);
			  }
			}
			if(cache == null){
				LOG.warn("Load finished. No data was found.");
				return null;
			}
			//TODO - disable cache loading for all IO workers except first one
			//  1. make init() public
			//  2. create bypass() ?
			// Or modify IOSaveThread to save meta only for first IO worker
			ioworkers[i].start();
		}		
		
		// start worker threads		
		LoadThread[] workers = new LoadThread[numWorkers];
		
		for(int i=0; i < numWorkers; i++)
		{
			workers[i] = new LoadThread(cache, filledBuffers, emptyBuffers);
			workers[i].start();
		}		
		
		// wait for IO threads to finish
		for(int i=0; i < numIO; i++)
		{
			while(ioworkers[i].isAlive()){
				try {
					ioworkers[i].join();
				} catch (InterruptedException e) {
				// TODO Auto-generated catch block	
					e.printStackTrace();
				}
			}			
		}		
		LOG.info("IO finished");
		
		for(int i=0; i < numWorkers; i++)
		{
			while(workers[i].isAlive()){
				LOG.info("Worker #"+i+" is alive");
				try {
					workers[i].finishWork();
					workers[i].join(100);
				} catch (InterruptedException e) {
				// TODO Auto-generated catch block	
					e.printStackTrace();
				}
			}			
		}
		LOG.info("Loading data finished. Time="+ 
		    (System.currentTimeMillis() - startTime)+"ms. Cache size: "+cache.size()+ 
		    " memory: "+cache.getTotalAllocatedMemorySize());
		return cache;
	}
	
	/**
	 * Gets the latest store file.
	 *
	 * @return the latest store file
	 */
	private final File getLatestStoreFile() {		
		String   lastSaved = diskStoreRoots[0]+File.separator+storeName+FILE_EXT;
		File lastSavedFile = new File(lastSaved);
		String lastSnapshot = diskStoreRoots[0]+File.separator+SNAPSHOTS +File.separator+ storeName+FILE_EXT;
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

	/** Load data from to. */
	private int totalInBuffer = 0;
	
	/**
	 * Load data from to.
	 *
	 * @param buf the buf
	 * @param offheap the offheap
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void loadDataFromTo(ByteBuffer buf, OffHeapCache offheap) throws IOException {
		
		totalInBuffer = 0;
		while(buf.remaining() > 0){
			try{
				loadChainedData(buf, offheap);
			}catch(Exception e){
				LOG.error("buf.position="+buf.position()+" buf.limit="+buf.limit()+" READ="+totalInBuffer);
				
				throw new IOException(e);
			}
		}
		LOG.info("Total read records "+totalInBuffer);
	}

	/**
	 * Load chained data.
	 *
	 * @param buf the buf
	 * @param cache the cache
	 * @throws NativeMemoryException the native memory exception
	 */
	private void loadChainedData(ByteBuffer buf, OffHeapCache cache) throws NativeMemoryException {
		long bufptr = cache.getBufferAddress();

		// Get initial bucket index in offheapcache buffer
		long index = buf.getLong();
		// Convert index to real address
		long address = bufptr + index*8;
		// get number of records in this bucket
		int numRecords = buf.getShort();// Max 32K record in one bucket
		int recordsLoaded = 0;
		while(recordsLoaded++ < numRecords){
			int start = buf.position();
			// advance by OFFSET
			buf.position(start +OffHeapCache.OFFSET );
			// Size of a record = OFFSET+ keyLength + valueLength
			int size = OffHeapCache.OFFSET + buf.getInt() + buf.getInt();
			long ptr = NativeMemory.malloc(size);
			// copy from buffer: position= current -OFFSET - 8;
			NativeMemory.memcpy(buf, start , size, ptr, 0);
			long nptr = ptr | 0x8000000000000000L;
			IOUtils.putLong(address, 0, nptr);
			address = ptr + OffHeapCache.NP_OFFSET;			
			// advance buffer position to next record
			buf.position(start + size);
			totalInBuffer++;
		}
		// set 0 as next ptr for last record
		IOUtils.putLong(address,0, 0);
		
	}



	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#put(java.nio.ByteBuffer, long)
	 */
	@Override
	public void put(ByteBuffer keyValue, long expTime) throws IOException {
		throw new UnsupportedOperationException("put");

	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#store(com.koda.cache.OffHeapCache, boolean, com.koda.persistence.ProgressListener)
	 */

	/**
	 * Store.
	 *
	 * @param memStore the mem store
	 * @param ignoreExpired the ignore expired
	 * @param pl the pl
	 * @param snapshot the snapshot
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void store(OffHeapCache memStore, boolean ignoreExpired,
			ProgressListener pl, boolean snapshot) throws IOException 
	{
		
		
		FileOutputStream fos = null;
		File temp = null;
        ReentrantReadWriteLock lock = null;
        ReadLock readLock = null;
        Codec codec = memStore.getCacheConfiguration().getDiskStoreConfiguration().
        											getDbCompressionType().getCodec();
		// Check that the store is in IDLE state
		if(!state.compareAndSet(State.IDLE, State.STORE)){
			throw new IOException("Can't store. Incompatible state.");
		}
		try {
			temp = snapshot? getTempFileForSnapshot(): getTempFileForStore();
			fos = new FileOutputStream(temp);

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
            long totalBuckets = memStore.getTotalBuckets();
            long current = 0;

            long totalMemorySize = memStore.getAllocatedMemorySize(); 
            
            LOG.info("After config: "+buffer.limit());
            
            while(current < totalBuckets )
            {
            	
            	if(snapshot){
            		// Acquire lock only during snapshot
            		lock = memStore.getLock(current);
            		readLock = lock.readLock();
            		readLock.lock();
            	}
               	
            	buffer.clear();
            	long old = current;
            	current = fillBuffer(buffer, memStore, current,
            			ignoreExpired, snapshot);
 
            	// Increase buffer size
            	if(old == current){
            		throw new IOException("buffer overflow");
            	}
            	
            	if(snapshot){
            		// Release lock
            		readLock.unlock();            	
            		readLock = null;
            		lock = null;
            	}
            	
            	buffer.flip();
            	if(codec != null){
            		LOG.info("[STORE] Buffer size="+(buffer.limit() -4));
            		// compress block
            		// skip blobSize
            		buffer.position(4);
            		tempBuffer.clear();
            		tempBuffer.position(4);
            		codec.compress(buffer, tempBuffer);
            		tempBuffer.putInt(0, tempBuffer.limit() - 4);
            		tempBuffer.position(0);
            		fileChannel.write(tempBuffer);
            		LOG.info("[STORE] Compressed="+(tempBuffer.limit()-4));
            	} else{            	
            		fileChannel.write(buffer); 
            	}
            	if(pl != null){
                   	long done = (long)((((double)current)/totalBuckets) * totalMemorySize);
                   	pl.progress(done, totalMemorySize);
            	}
            	// check canceled
            	if(isCanceled()){
            		if(pl != null){  pl.canceled(); }
            		throw new RuntimeException("canceled");
            	}
            }            			
			
		} catch (Exception e) {
			
			if (pl != null) { pl.error(e, true); }				
			throw new IOException(e);
			
		}  finally {

			if(isCanceled()) { cancelRequested = false; }			
			if(fileChannel != null && fileChannel.isOpen()){
				fileChannel.close();
			}
			//TODO
			state.compareAndSet(State.STORE, State.IDLE);			
			if(readLock != null){ 	readLock.unlock(); }			
			// Rename file
			File to = snapshot? getFileForSnapshot(): getFileForStore();
			temp.renameTo(to);
		}
		// Notify listener we are done.
		if(pl != null){ pl.finished(); }

	}

	
	/**
	 * Temp implementation.
	 *
	 * @param memStore the mem store
	 * @param ignoreExpired the ignore expired
	 * @param pl the pl
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void storeFast(OffHeapCache memStore, boolean ignoreExpired, ProgressListener pl)
		throws IOException
	{
		long startTime = System.currentTimeMillis();
		
		LOG.info("Saving data started");
		int numWorkers = config.getTotalWorkerThreads();
		int numIO = config.getTotalIOThreads();
		if(numIO != diskStoreRoots.length) 
		  numIO = diskStoreRoots.length;
		int bufSize = config.getRWBufferSize();
		LOG.info("[store_fast] Workers    ="+numWorkers);
		LOG.info("[store_fast] IO threads ="+numIO);
		
		ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
		for(int i=0; i < numWorkers; i++){
			ByteBuffer buf = ByteBuffer.allocateDirect(bufSize);
			buf.order(ByteOrder.nativeOrder());
			buffers.add(buf);
		}
		
		ArrayBlockingQueue<ByteBuffer> emptyBuffers = 
			new ArrayBlockingQueue<ByteBuffer>(numWorkers, false, buffers);
		ArrayBlockingQueue<ByteBuffer> filledBuffers = 
			new ArrayBlockingQueue<ByteBuffer>(numWorkers);
		
		// start worker threads
		SaveThread[] workers = new SaveThread[numWorkers];
		
		for(int i=0; i < numWorkers; i++)
		{
			workers[i] = new SaveThread(i, numWorkers, memStore, emptyBuffers, filledBuffers);
			workers[i].setIgnoreExpired(ignoreExpired);
			workers[i].start();
		}		
		// start IO threads

		IOSaveThread[] ioworkers = new IOSaveThread[numIO];
		
		for(int i=0; i < numIO; i++)
		{
			ioworkers[i] = new IOSaveThread(memStore, diskStoreRoots[i], filledBuffers, emptyBuffers, pl);
			ioworkers[i].start();
		}		
		
		// wait for all to finish
		for(int i=0; i < numWorkers; i++)
		{
			while(workers[i].isAlive()){
				try {
					workers[i].join();
				} catch (InterruptedException e) {
				// TODO Auto-generated catch block				
				}
			}			
		}		
		
		for(int i=0; i < numIO; i++)
		{
			while(ioworkers[i].isAlive()){
				try {
					ioworkers[i].finishWork();
					ioworkers[i].join(100);
				} catch (InterruptedException e) {
				// TODO Auto-generated catch block				
				}
			}			
		}
		LOG.info("Saving data finished. Time="+ (System.currentTimeMillis() - startTime)+"ms");
	}
	
	/**
	 * Gets the temp file for snapshot.
	 *
	 * @return the temp file for snapshot
	 */
	private File getTempFileForSnapshot() {

		String lastSnapshot = diskStoreRoots[0]+File.separator+SNAPSHOTS +File.separator+ storeName+FILE_EXT+".current";
		File lastSnapshotFile = new File(lastSnapshot);
		return lastSnapshotFile;
	}

	/**
	 * Gets the temp file for store.
	 *
	 * @return the temp file for store
	 */
	private File getTempFileForStore() {
		String   current = diskStoreRoots[0]+File.separator+storeName+FILE_EXT+".current";
		File currentForSave = new File(current);
		return currentForSave;
	}
	
	/**
	 * Gets the file for snapshot.
	 *
	 * @return the file for snapshot
	 */
	private File getFileForSnapshot() {

		String lastSnapshot = diskStoreRoots[0]+File.separator+SNAPSHOTS +File.separator+ storeName+FILE_EXT;
		File lastSnapshotFile = new File(lastSnapshot);
		return lastSnapshotFile;
	}

	/**
	 * Gets the file for store.
	 *
	 * @return the file for store
	 */
	private File getFileForStore() {
		String   current = diskStoreRoots[0]+File.separator+storeName+FILE_EXT;
		File currentForSave = new File(current);
		return currentForSave;
	}

	/**
	 * Load from snapshot.
	 *
	 * @return true, if successful
	 */
	private boolean loadFromSnapshot()
	{
		File snapFile = getFileForSnapshot();
		// TODO we check only [0] store directory.
		// There is no check that last shutdown was clean
		File storeFile= getFileForStore();
		if(!snapFile.exists()) return false;
		if(!storeFile.exists()) return true;
		return snapFile.lastModified() > storeFile.lastModified();
	}
	
	/**
	 * Gets the snapshot dir.
	 *
	 * @return the snapshot dir
	 */
	private String getSnapshotDir()
	{
		return diskStoreRoots[0]+File.separator+SNAPSHOTS;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#store(com.koda.cache.OffHeapCache, boolean, com.koda.persistence.ProgressListener)
	 */
	@Override
	public void store(OffHeapCache memStore, boolean ignoreExpired,
			ProgressListener pl) throws IOException 
	{
		store(memStore, ignoreExpired, pl, false);
	}
	
	/**
	 * Fill buffer.
	 *
	 * @param buf the buf
	 * @param memStore the mem store
	 * @param current the current
	 * @param ignoreExpired the ignore expired
	 * @param snapshot the snapshot
	 * @return the int
	 * @throws NativeMemoryException the native memory exception
	 */
	private long fillBuffer(ByteBuffer buf, OffHeapCache memStore, 
			long curr,  boolean ignoreExpired, boolean snapshot ) throws NativeMemoryException {

		long bufptr = memStore.getBufferAddress();
		long limit = memStore.getTotalBuckets() * 8;
		buf.position(4);
		if(snapshot){
			// Make sure that we do not cross
			// lock stripe boundary.
			// Adjust 'limit' accordingly
			long bucket = ((long)curr * OffHeapCache.getLockStripesCount())/limit;
			limit = (limit/ OffHeapCache.getLockStripesCount())*(bucket+1);
		}
		long current = curr;		
		for(; current < limit; current++){
				long ptr = IOUtils.getLong(bufptr, current * 8);
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
			if(ignoreExpired){
				if(memStore.isExpiredEntry(ptr)){
					expired = true;
				}
			}
			int size = IOUtils.getRecordSize(ptr/*+OffHeapCache.OFFSET*/);
			size += OffHeapCache.OFFSET;
			
			if(!expired) NativeMemory.memcpy(ptr, 0, buf, offset, size);			
			// next ptr
			ptr = IOUtils.getLong(ptr, OffHeapCache.NP_OFFSET);
			total ++;
			offset += size;			
		}
		
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
			size += IOUtils.getRecordSize(ptr/*+OffHeapCache.OFFSET*/) +OffHeapCache.OFFSET;
			ptr = IOUtils.getLong(ptr, OffHeapCache.NP_OFFSET);
		}
		return size;
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#storeAsync(com.koda.cache.OffHeapCache, boolean, com.koda.persistence.ProgressListener)
	 */
	@Override
	public void storeAsync(final OffHeapCache memStore, final boolean ignoreExpired,
			final ProgressListener pl) throws IOException {
		Runnable r = new Runnable(){
			public void run()
			{
				try{
					store(memStore, ignoreExpired, pl);
				}catch(IOException e)
				{
					LOG.error(e);
				}
			}
		};
		
		new Thread(r,"AsyncStore").start();


	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#storeSnapshotAsync(com.koda.cache.OffHeapCache, boolean, com.koda.persistence.ProgressListener)
	 */
	@Override
	public void startSnapshots(
			final boolean ignoreExpired, final ProgressListener pl) throws IOException {

		if(!config.isSnapshotsEnabled()){
			throw new IOException("snapshots are disabled in configuration");
		}
		if(snapshotsActivated) {
			LOG.warn("[RawFSStore::startSnapshots] - already started");
			return;
		}
		
		this.snapshotsActivated = true;
		

		
		Runnable r = new Runnable(){
			public void run()
			{
				LOG.info("Snapshot thread started. Interval is "+config.getDbSnapshotInterval()+" sec");
				
				try{
					while(snapshotsActivated){
						long time =0L;
						try {
							long timeout = config.getDbSnapshotInterval()*1000 - time;
							if(timeout < 0) timeout = 0;
							LOG.info("Start sleep. timeout ="+timeout);
							Thread.sleep(timeout);
						} catch (InterruptedException e) {
							if(snapshotsActivated == false) break;
						}
						// do snapshot
						if(getState() == State.IDLE){
							LOG.info("Snapshot started");
							long start = System.currentTimeMillis();
							store(cache, ignoreExpired, pl, true);
							time = System.currentTimeMillis() - start;
							LOG.info("Snapshot done in "+time+" ms");
							
						}
					}
					LOG.info("Snapshot thread stopped");
				}catch(IOException e)
				{
					LOG.error(e);
				}
			}
		};
		
		this.snapshotThread = new Thread(r,"AsyncStore:: snapshot");
		this.snapshotThread.start();

	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#areRowOperationsSupported()
	 */
	@Override
	public boolean areRowOperationsSupported() {
		// we do not support row-level operations
		return false;
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#isSnapshotsEnabled()
	 */
	@Override
	public boolean isSnapshotsEnabled() throws IOException {
		
		return snapshotsActivated;
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.DiskStore#stopSnapshots()
	 */
	@Override
	public void stopSnapshots() throws IOException {
		LOG.info("Stop snapshots");
		snapshotsActivated = false;	
		try {
			if(snapshotThread.isAlive()){
				snapshotThread.interrupt();
				snapshotThread.join();
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}

}

