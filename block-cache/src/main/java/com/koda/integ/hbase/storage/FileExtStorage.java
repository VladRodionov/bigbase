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
package com.koda.integ.hbase.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.koda.NativeMemory;
import com.koda.cache.OffHeapCache;
import com.koda.util.CLib;
import com.koda.util.CLibrary;


// TODO: Auto-generated Javadoc
/**
 * The Class FileExtStorage.
 * 
 * Current replacement policy is SC-FIFO
 * 
 * TODO: LRU replacement policy
 * 
 * 
 * 1. Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner") - to ummap mapped BB
 * 
 * public void unmapMmaped(ByteBuffer buffer) {
  if (buffer instanceof sun.nio.ch.DirectBuffer) {
    sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) buffer).cleaner();
    cleaner.clean();
  }
}
 * 2. mlockall - JNA
 * 3. low level I/I (JNA) posix_fadvise, mincore/fincore fctl
 * 4. 
 * 
 */
public class FileExtStorage implements ExtStorage {
	  /** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(FileExtStorage.class);
	  
	/** The Constant FILE_STORAGE_BASE_DIR. */
	public final static String FILE_STORAGE_BASE_DIR = "offheap.blockcache.file.storage.baseDir";	
	
	/** The Constant FILE_STORAGE_MAX_SIZE. */
	public final static String FILE_STORAGE_MAX_SIZE = "offheap.blockcache.file.storage.maxSize";	
	
	/** The Constant FILE_STORAGE_BUFFER_SIZE. */
	public final static String FILE_STORAGE_BUFFER_SIZE = "offheap.blockcache.file.storage.bufferSize";
	
	/** The Constant FILE_STORAGE_NUM_BUFFERS. */
	public final static String FILE_STORAGE_NUM_BUFFERS = "offheap.blockcache.file.storage.numBuffers";
	
	/** The Constant FILE_STORAGE_FLUSH_INTERVAL. */
	public final static String FILE_STORAGE_FLUSH_INTERVAL = "offheap.blockcache.file.storage.flushInterval";
	
	/** The Constant FILE_STORAGE_FILE_SIZE_LIMIT. */
	public final static String FILE_STORAGE_FILE_SIZE_LIMIT = "offheap.blockcache.file.storage.fileSizeLimit";
	
	/** Second Chance FIFO ratio. */
	public final static String FILE_STORAGE_SC_RATIO = "offheap.blockcache.file.storage.fifoRatio";
	
	public final static String FILE_STORAGE_PAGE_CACHE = "offheap.blockcache.file.storage.pagecache";
	
	public final static String DATA_FILE_NAME_PREFIX = "data-";
	
	/** The Constant DEFAULT_BUFFER_SIZE. */
	private final static int DEFAULT_BUFFER_SIZE = 8*1024*1024;
	
	/** The Constant DEFAULT_NUM_BUFFERS. */
	private final static int DEFAULT_NUM_BUFFERS = 10;
	
	/** The Constant DEFAULT_SC_RATIO. */
	private final static float DEFAULT_SC_RATIO  = 0.f;
	
	/** The Constant DEFAULT_FLUSH_INTERVAL. */
	private final static long  DEFAULT_FLUSH_INTERVAL = 30000; // in ms
	
	/** The Constant DEFAULT_FILE_SIZE_LIMIT. */
	private final static long DEFAULT_FILE_SIZE_LIMIT = 2 * 1000000000;
	
	/** The Constant DEFAULT_BUFFER_SIZE_STR. */
	private final static String DEFAULT_BUFFER_SIZE_STR = Integer.toString(DEFAULT_BUFFER_SIZE);
	
	/** The Constant DEFAULT_NUM_BUFFERS_STR. */
	private final static String DEFAULT_NUM_BUFFERS_STR = Integer.toString(DEFAULT_NUM_BUFFERS);
	
	/** The Constant DEFAULT_SC_RATIO_STR. */
	private final static String DEFAULT_SC_RATIO_STR  = Float.toString(DEFAULT_SC_RATIO);
	
	/** The Constant DEFAULT_FLUSH_INTERVAL_STR. */
	private final static String DEFAULT_FLUSH_INTERVAL_STR = Long.toString(DEFAULT_FLUSH_INTERVAL);
	
	/** The Constant DEFAULT_FILE_SIZE_LIMIT_STR. */
	private final static String DEFAULT_FILE_SIZE_LIMIT_STR = Long.toString(DEFAULT_FILE_SIZE_LIMIT);
	
	private final static String DEFAULT_PAGE_CACHE = Boolean.toString(true);
	
	/** The base storage dir. */
	private String fileStorageBaseDir;	
	/** The max storage size. */
	private long maxStorageSize ;	
	
	/** The buffer size. */
	private int bufferSize = DEFAULT_BUFFER_SIZE;	
	
	/** The num buffers. */
	private int numBuffers = DEFAULT_NUM_BUFFERS;
	
	/** The flush interval. */
	private long flushInterval = DEFAULT_FLUSH_INTERVAL; 
	
	/** The second chance fifo ratio. */
	private float secondChanceFIFORatio = DEFAULT_SC_RATIO;
	
	/** The file size limit. */
	private long fileSizeLimit = DEFAULT_FILE_SIZE_LIMIT;
	
	/** The max id. */
	// TODO - long
	private AtomicInteger maxId = new AtomicInteger(0);	
	
	/** The max id for writes. */
	private AtomicInteger maxIdForWrites = new AtomicInteger(0);
	
	/** The min id. */
	private AtomicInteger minId = new AtomicInteger(0);
	
	/** The current storage size. */
	private AtomicLong currentStorageSize = new AtomicLong(0);
	
	/** The existed ids. */
	private ConcurrentHashMap<Long,Long> existedIds = new ConcurrentHashMap<Long, Long>();
	
	/** Multiple readers. */
	private ConcurrentHashMap<Integer, Queue<RandomAccessFile>> readers = 
		new ConcurrentHashMap<Integer, Queue<RandomAccessFile>>();
	
	/** Just one writer - current file. */
	private RandomAccessFile currentForWrite;
	
	/** Filled buffer queue. */
	private ArrayBlockingQueue<ByteBuffer> writeQueue ; 
		
	/** Empty buffers queue. */
	private ArrayBlockingQueue<ByteBuffer> emptyBuffersQueue;
	
	/** The buffer offset. */
	private AtomicLong bufferOffset = new AtomicLong(0);
	
	/** The current file offset. */
	private AtomicLong currentFileOffset = new AtomicLong(0);
	
	/** The current file offset for writes. */
	private AtomicLong currentFileOffsetForWrites = new AtomicLong(0);
	
	/** The active buffer. */
	private AtomicReference<ByteBuffer> activeBuffer = new AtomicReference<ByteBuffer>(null);
		
	/** Maximum open file descriptors for file. */
	private int maxOpenFD = 5;
	
	/** no_page_cache */
	private boolean noPageCache = false;
	
	/** We do have one lock. */
	ReentrantReadWriteLock writeLock = new ReentrantReadWriteLock();
	
	/** The locks. */
	ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[1024];
	
	/** Data buffer flush thread. */
	
	private FileFlusher flusher;
	
	/** The Recycler - garbage collector (object evictor). */
	private Thread recycler;
	
	/** The configuration object. */
	private Configuration config;

	/** The block cache keeps external storage references. */
	private OffHeapCache storageRefCache;
	
	/**
	 * The Class FileFlusher.
	 */
	class FileFlusher extends Thread
	{
		
		/**
		 * Instantiates a new file flusher.
		 */
		public FileFlusher(){
			super("file-flusher");
			LOG.info("File flusher thread started.");
		}
		
		
		
		/* (non-Javadoc)
		 * @see java.lang.Thread#run()
		 */
		public void run()
		{
			LOG.info(Thread.currentThread().getName()+" started at "+ new Date());

			while(true){
				try {
				  
          // Check if need to start storage recycle thread (GC thread)
          checkRecyclerThread();
          // Wait if storage is full
          // This is anomaly event which must be logged.
          waitUntilStorageHasRoom();
          
					ByteBuffer buf = writeQueue.poll(flushInterval, TimeUnit.MILLISECONDS);
					
					if(Thread.currentThread().isInterrupted()){
					  if(writeQueue.size() == 0  && buf == null) {
						  LOG.info(Thread.currentThread().getName() + " exited.");
						  return;
						}
					}
					
					if(buf == null){
						// write queue is empty. All buffers in empty queue
						// flash current active
						// TODO activeBuffer.
						writeLock.writeLock().lockInterruptibly();
						try{
							if(bufferOffset.get() == 0) {
								continue;
							}
							buf = activeBuffer.get();							
							if(buf.position() > 0) buf.flip();							
							bufferOffset.set(0);
							// Its a blocking call but as since buf== null - all buffers are 
							// in the empty queue.
							activeBuffer.set(emptyBuffersQueue.take());

						} catch(Exception e){
							// TODO
							LOG.error(e);
							continue;
						} finally{
							writeLock.writeLock().unlock();
						}						
					} else{
						if(buf.position() > 0) buf.flip();
					}	
					
					
					//verifyBuffer(buf);
					
					int toWrite = buf.limit();
					checkCurrentFileSize(toWrite);
					
					
					ReentrantReadWriteLock fileLock = getCurrentFileLock();
					fileLock.writeLock().lockInterruptibly();
					try{
						// save data to  file and sync
						
						FileChannel fc = currentForWrite.getChannel();
						int written = 0;
						while(written < toWrite){
							written += fc.write(buf);
						}
						
						fc.force(true);
						if( written != toWrite){
							LOG.error("fc written= "+ written+" expected "+toWrite);
						}
						currentFileOffset.addAndGet(written);
					} finally{
						fileLock.writeLock().unlock();
					}

					// return buffer back to empty buffer list
					// offer will always succeed
					buf.clear();
					emptyBuffersQueue.offer(buf);
					
				} catch (InterruptedException e) {
				  // TODO do we need global variable to check 'closing' state?
						if(writeQueue.size() > 0) continue;
						LOG.info(Thread.currentThread().getName() + " exited.");
						return;
				} catch(Throwable e){
					LOG.fatal(e);
					e.printStackTrace();
					LOG.fatal(Thread.currentThread().getName()+" thread died.");
					//System.exit(-1);
				}
			}
		}
		
		/**
		 * Wait until storage has room.
		 */
		private void waitUntilStorageHasRoom() {
      long startTime = System.currentTimeMillis();
      long lastLoggedTime = startTime;
		  while( currentStorageSize.get() > maxStorageSize - bufferSize){        
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
        }   
        
        // WE NEED TO LOGG if 
        if(System.currentTimeMillis() - lastLoggedTime > 1000){
          LOG.warn("[waitUntilStorageHasRoom] File flusher is waiting for "+(System.currentTimeMillis() - startTime)+"ms");
          lastLoggedTime = System.currentTimeMillis();
        }
      }
		  long endTime = System.currentTimeMillis();
		  if(endTime - startTime > 1000){
		     LOG.warn("[waitUntilStorageHasRoom] File flusher waited "+(endTime - startTime)+"ms");
		  }
    }



    /**
		 * Check current file size.
		 *
		 * @param toWrite the to write
		 * @throws IOException Signals that an I/O exception has occurred.
		 */
		private void checkCurrentFileSize(int toWrite) throws IOException
		{
			if(currentForWrite == null){
				
				String path = getFilePath(maxId.get());
				LOG.info("Create new file: "+path);
				currentForWrite = new RandomAccessFile(path, "rws");
				existedIds.put((long)maxId.get(), (long)maxId.get());
				
			}
			if( currentFileOffset.get() + bufferSize > fileSizeLimit){
				long size = currentForWrite.length();
				currentForWrite.close();

				maxId.incrementAndGet();
				String path = getFilePath(maxId.get());
				LOG.info("Creating "+path);
				currentForWrite = new RandomAccessFile(path, "rws");
				currentStorageSize.addAndGet(size);

				//currentFileOffset.set(toWrite);
				currentFileOffset.set(0);
				existedIds.put((long)maxId.get(), (long)maxId.get());
			} else{
				//currentFileOffset.addAndGet(toWrite);
			  return;
			}
	    if( noPageCache == true){
	      int fd = CLibrary.getfd(currentForWrite.getFD());
	      CLib.trySkipCache(fd, 0, 0);
	    }
		}


	}
	/**
	 * Delete oldest file.
	 */
	public void deleteOldestFile() {
		
		LOG.info("[FileExtStorage] exceeded storage limit of "+maxStorageSize+". Deleting "+getFilePath(minId.get()));
		File f = new File(getFilePath(minId.get()));
		long fileLength = f.length();
		boolean result = f.delete();
		if(result == false){
			LOG.fatal("[FileExtStorage] Deleting "+getFilePath(minId.get())+" failed.");
		} else{
			LOG.info("[FileExtStorage] Deleting "+getFilePath(minId.get())+" succeeded.");	
			// Increment min id.
			Queue<RandomAccessFile> files = readers.remove(minId.get());
			// Remove from existed
			existedIds.remove(minId.get());
			
			if(files != null){
				for(RandomAccessFile file: files){
			
				  try{
						file.close();
					} catch(Exception e){
						// ignore?
					}
				}
			}

			currentStorageSize.addAndGet(-fileLength);
			minId.incrementAndGet();
			
		}			
	}
	
	/**
	 * Check recycler thread.
	 */
	public void checkRecyclerThread() {
		if(recycler != null && recycler.isAlive()) {
			// 1. Running
			return;
		}
		// Check if we need to start storage recycler thread
		float highWatermark = Float.parseFloat(config.get(StorageRecycler.STORAGE_RATIO_HIGH_CONF, 
				StorageRecycler.STORAGE_RATIO_HIGH_DEFAULT));
		long currentSize = getCurrentStorageSize();
		long maxSize = getMaxStorageSize();
		float currentRatio = ((float)currentSize)/ maxSize;
		if(currentRatio >= highWatermark){
			// Start recycler thread
		  StorageRecycler sr = StorageRecyclerManager.getInstance().getStorageRecycler(config);
		  sr.set(this);
			recycler = (Thread) sr;
			recycler.start();
		}
		
	}
	
	/**
	 * Gets the storage ref cache.
	 *
	 * @return the storage ref cache
	 */
	public OffHeapCache getStorageRefCache()
	{
	  return storageRefCache;
	}
	
	/**
	 * Gets the current file lock.
	 *
	 * @return the current file lock
	 */
	private ReentrantReadWriteLock getCurrentFileLock()
	{
		return locks[maxId.get() % locks.length];
	}
	
	
	/**
	 * Get existing file.
	 *
	 * @param id the id
	 * @return file
	 */
	
	public RandomAccessFile getFile(int id)
	{

		if( existedIds.containsKey((long) id) == false){
			return null;
		}
		
		Queue<RandomAccessFile> fileReaders = readers.get(id);
		if(fileReaders == null){
		  if(existedIds.containsKey((long) id) == false){
		    return null;
		  }
			fileReaders = new ArrayBlockingQueue<RandomAccessFile>(maxOpenFD);
			readers.putIfAbsent(id, fileReaders);
		}
		fileReaders = readers.get(id);
		
		if(fileReaders == null){
		  return null;
		}
		
		RandomAccessFile raf = fileReaders.poll();
		
		if( raf == null ){	
			raf = openFile(id, "r");
		}
		return raf;
	}	
	
	/**
	 * Open file.
	 *
	 * @param id the id
	 * @param mode the mode
	 * @return the random access file
	 */
	private RandomAccessFile openFile(int id, String mode)  {
		String path = getFilePath(id);
		RandomAccessFile file = null;
		try {
			file = new RandomAccessFile(path, mode);
			if(noPageCache == true){
			  int fd = CLibrary.getfd(file.getFD());
			  CLib.trySkipCache(fd, 0, 0);
			}
		} catch (FileNotFoundException e) {

		} catch (IOException e) {
		  LOG.error(e.getMessage(), e);
    }
		return file;
	}

	/**
	 * Gets the file path.
	 *
	 * @param id the id
	 * @return the file path
	 */
	public String getFilePath(int id) {
		String path = fileStorageBaseDir + File.separator + DATA_FILE_NAME_PREFIX+format(id, 10);
		return path;
	}
	
	/**
	 * Format.
	 *
	 * @param id the id
	 * @param positions the positions
	 * @return the string
	 */
	private String format (int id, int positions)
	{
		String s = Integer.toString(id);
		int n = positions - s.length();
		if(n < 0) return s.substring(0, positions);
		for(int i=0; i < n; i++){
			s = "0"+s;
		}
		return s;
	}

	/**
	 * Gets the id from file name.
	 *
	 * @param name the name
	 * @return the id from file name
	 */
	private int getIdFromFileName(String name)
	{
		int i = name.lastIndexOf("-");
		String sid = name.substring(i + 1);
		return Integer.parseInt(sid, 10);
	}
	
	/**
	 * Put file.
	 *
	 * @param id the id
	 * @param file the file
	 */
	private void putFile(int id, RandomAccessFile file) {
		Queue<RandomAccessFile> fileReaders = readers.get(id);
		if (fileReaders == null) {
			fileReaders = new ArrayBlockingQueue<RandomAccessFile>(maxOpenFD);
			readers.putIfAbsent(id, fileReaders);
		}
		fileReaders = readers.get(id);
		boolean result = fileReaders.offer(file);
		if (result == false) {
			try {
				file.close();
			} catch (IOException e1) {
				LOG.error(e1);
			}
		}
	}
	
	/**
	 * Instantiates a new file ext storage.
	 */
	public FileExtStorage(){}
	
	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.ExtStorage#config(org.apache.hadoop.conf.Configuration)
	 */
	@Override
	public void config(Configuration config, OffHeapCache cache) throws IOException {

		this.storageRefCache = cache;
		initConfig(config);
		dumpConfig();
		initQueues();
		// check number of files and calculate maxId, currentStorage size
		checkFilesInStorage();
		// Start flush thread
		flusher = new FileFlusher();
		flusher.start();
		
	}
	
	/**
	 * Check files in storage.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void checkFilesInStorage() throws IOException {
		
		LOG.info("[FileExtStorage] initializing storage : "+fileStorageBaseDir);
		File storageDir = new File(fileStorageBaseDir);
		String[] files = storageDir.list( new FilenameFilter(){

			@Override
			public boolean accept(File f, String name) {
				return name.startsWith(DATA_FILE_NAME_PREFIX);
			}
			
		});
		if(files == null) {
			LOG.info("Creating "+fileStorageBaseDir);
			if( storageDir.mkdirs() == false){
				throw new IOException(fileStorageBaseDir +" does not exists and can't be created.");
			}
		}
		LOG.info("[FileExtStorage] found : "+ ((files !=null)?files.length:0)+" files.");

		if(files == null || files.length == 0) return;
		
		Arrays.sort(files);
		
		// Do storage size
		for(String s: files){
			File f = new File(fileStorageBaseDir + File.separator + s);
			currentStorageSize.addAndGet(f.length());			
		}
		LOG.info("[FileExtStorage] total size : "+currentStorageSize);

		// Do maxId
		if(files.length > 0){
			maxId.set(getIdFromFileName(files[files.length -1]) + 1);
			maxIdForWrites.set(maxId.get());
			minId.set(getIdFromFileName(files[0]));
		}
		
		// Init existedIds set
		for(String s: files){
			long id = getIdFromFileName(s);
			existedIds.put(id, id);
		}
		
		LOG.info("[FileExtStorage] max id : "+ maxId+" min id: "+minId);		
		
	}

	/**
	 * Inits the config.
	 *
	 * @param cfg the cfg
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private void initConfig(Configuration cfg) throws IOException{

		LOG.info("[FileExtStorage] init config ...");
		this.config = cfg;
		String value = cfg.get(FILE_STORAGE_BASE_DIR);
		if( value == null){
			throw new IOException("[FileExtStorage] Base directory not specified.");
		}
		fileStorageBaseDir = value;
		value = cfg.get(FILE_STORAGE_MAX_SIZE);
		if( value == null){
			throw new IOException("[FileExtStorage] Maximum storage size not specified.");
		} else{
			maxStorageSize = Long.parseLong(value);
		}
		value = cfg.get(FILE_STORAGE_BUFFER_SIZE, DEFAULT_BUFFER_SIZE_STR);
		bufferSize = Integer.parseInt(value);
		value = cfg.get(FILE_STORAGE_NUM_BUFFERS, DEFAULT_NUM_BUFFERS_STR);
		numBuffers = Integer.parseInt(value);
		value = cfg.get(FILE_STORAGE_FLUSH_INTERVAL, DEFAULT_FLUSH_INTERVAL_STR);
		flushInterval = Long.parseLong(value);		
		value = cfg.get(FILE_STORAGE_SC_RATIO, DEFAULT_SC_RATIO_STR);
		secondChanceFIFORatio = Float.parseFloat(value);	
		value = cfg.get(FILE_STORAGE_FILE_SIZE_LIMIT, DEFAULT_FILE_SIZE_LIMIT_STR);
		fileSizeLimit = Long.parseLong(value);	
		noPageCache = !cfg.getBoolean(FILE_STORAGE_PAGE_CACHE, Boolean.parseBoolean(DEFAULT_PAGE_CACHE));
		
		// init locks
		for(int i=0; i < locks.length; i++){
			locks[i] = new ReentrantReadWriteLock();
		}
		
	}

	
	/**
	 * Dump config.
	 */
	private void dumpConfig()
	{
		LOG.info("Block cache file storage:");
		LOG.info("Base dir        : "+ fileStorageBaseDir);
		LOG.info("Max size        : "+ maxStorageSize);
		LOG.info("Buffer size     : "+ bufferSize);
		LOG.info("Num buffers     : "+ numBuffers);
		LOG.info("Flush interval  : "+ flushInterval);
		LOG.info("FIFO  ratio     : "+ secondChanceFIFORatio);
		LOG.info("File size limit : "+ fileSizeLimit);
		
	}
	
	/**
	 * Inits the queues.
	 */
	private void initQueues()
	{
		writeQueue = new ArrayBlockingQueue<ByteBuffer>(numBuffers);
		emptyBuffersQueue = new ArrayBlockingQueue<ByteBuffer>(numBuffers);
		for(int i = 0; i < numBuffers; i++)
		{
			ByteBuffer buf = NativeMemory.allocateDirectBuffer(16, bufferSize);
			emptyBuffersQueue.add(buf);
		}
		try {
			activeBuffer.set(emptyBuffersQueue.take());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			LOG.warn(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.ExtStorage#getData(com.koda.integ.hbase.storage.StorageHandle, java.nio.ByteBuffer)
	 */
	@Override
	public StorageHandle getData(StorageHandle storeHandle, ByteBuffer buf) {
		FileStorageHandle fsh = (FileStorageHandle) storeHandle;

		// Check if current file and offset > currentFileOffset
		int id = maxId.get();
		if(fsh.getId() > id || ( fsh.getId() == id && fsh.getOffset() >= currentFileOffset.get() )){
			// not found
			buf.putInt(0,0);
			return fsh;
		}
		
		//ReentrantReadWriteLock lock = locks[fsh.getId() % locks.length];
		
		//lock.writeLock().lock();
		
		RandomAccessFile file = getFile(fsh.getId());//openFile(fsh.getId(), "r");
		
		boolean needSecondChance = needSecondChance(fsh.getId());
		
		try {
			if (file == null) {
				// return null
				buf.putInt(0, 0);
			} else {
				buf.clear();
				int toRead = fsh.getSize();
				buf.putInt(fsh.getSize());
				buf.limit(4 + toRead);
				try {
					FileChannel fc = file.getChannel();
					int total = 0;
					int c = 0;
					// offset start with overall object length .add +4
					int off = fsh.getOffset() + 4;
					while (total < toRead) {
						c = fc.read(buf, off);
						off += c;
						if (c < 0) {
							// return not found
							buf.putInt(0, 0);
							break;
						}
						total += c;
					}
				} catch (IOException e) {
					//LOG.error(e);
					// return not found
					if(fsh.getId() > minId.get()){
						e.printStackTrace();
					}
					buf.putInt(0, 0);
				}
			}
			if(buf.getInt(0) != 0 && needSecondChance){
				// store again
				fsh = (FileStorageHandle) storeData(buf);
			}
			return fsh;
			
		} finally {
			if(file != null){
				// return file back
				
			  
			  
			  // PUT we need for old version
			  putFile(fsh.getId(), file);
				
				
				
//				try {
//					file.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					//e.printStackTrace();
//				}
			}
			//lock.writeLock().unlock();
		}

	}

	/**
	 * Need second chance.
	 *
	 * @param id the id
	 * @return true, if successful
	 */
	private boolean needSecondChance(int id) {
		if(minId.get() == 0 || secondChanceFIFORatio == 0.f) return false;
		if( maxId.get() - minId.get() > 0){
			float r = ((float)(id - minId.get() )) / (maxId.get() - minId.get());
			if( r < secondChanceFIFORatio) return true;
		}
		return false;
	}
	
	/**
	 * Stores multiple objects in one transaction
	 * Format of a buffer:
	 * 0..3 - total size of a batch
	 * 4.. - batch of blocks
	 *
	 * @param buf the buf
	 * @return the list
	 */
	public List<StorageHandle> storeDataBatch(ByteBuffer buf)
	{
		List<StorageHandle> handles = storeDataNoReleaseLock(buf);
		if( handles == null) {
			
			handles = new ArrayList<StorageHandle>();
			
			int size = buf.getInt(0);
			buf.position(4);

			while(buf.position() < size + 4){
				buf.limit(buf.capacity());
				StorageHandle fsh = storeData(buf);
				handles.add(fsh);
			}
		}
		return handles;
		
	}
	
	/**
	 * Tries to store batch of blocks into a current buffer.
	 *
	 * @param buf the buf
	 * @return the list
	 */
	private List<StorageHandle> storeDataNoReleaseLock(ByteBuffer buf) {
		
		List<StorageHandle> handles = new ArrayList<StorageHandle>();		
		writeLock.writeLock().lock();
		try{

			if( activeBuffer.get() == null){
				return null;
			}
			int size = buf.getInt(0);			
			long off = bufferOffset.get();
			if(off + size > bufferSize){
				return null;
			}
			
			long currentFileLength = currentFileOffsetForWrites.get();
			if(bufferOffset.get() == 0 && currentFileLength + bufferSize > fileSizeLimit){
				// previous buffer was flushed
				currentFileOffsetForWrites.set(0);
				maxIdForWrites.incrementAndGet();				
			}
			
			buf.position(4);

			while(buf.position() < size + 4){
				buf.limit(buf.capacity());
				int pos = buf.position();
				int blockSize = buf.getInt();
				buf.position(pos);
				buf.limit(pos + 4 + blockSize);			
				activeBuffer.get().put(buf);			
				FileStorageHandle fsh = new FileStorageHandle(maxIdForWrites.get(), (int)(currentFileOffsetForWrites.get()), blockSize);
				handles.add(fsh);
				// Increase offset in current file for writes;
				currentFileOffsetForWrites.addAndGet(blockSize + 4);
				bufferOffset.getAndAdd(blockSize + 4);
			}
			return handles;
		} finally{
			WriteLock lock = writeLock.writeLock();
			if(lock.isHeldByCurrentThread()){
				lock.unlock();
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.ExtStorage#storeData(long)
	 */
	@Override
	public StorageHandle storeData(ByteBuffer buf) {
		writeLock.writeLock().lock();
		int pos = 0;
		try{

			if( activeBuffer.get() == null){
				// unlock
				writeLock.writeLock().unlock();
				// Get next buffer from empty queue - blocking call
				ByteBuffer bbuf = emptyBuffersQueue.take();
				// lock again
				writeLock.writeLock().lock();
				if(activeBuffer.get() == null){
					activeBuffer.set(bbuf);
					bufferOffset.set(0);
				} else {
					//  somebody already set the activeBuffer
					// repeat call recursively
					emptyBuffersQueue.offer(bbuf);
					writeLock.writeLock().unlock();
					return storeData(buf);
				}
			}
			
			
			pos = buf.position();
			long currentFileLength = currentFileOffsetForWrites.get();
			if(bufferOffset.get() == 0 && currentFileLength + bufferSize > fileSizeLimit){
				// previous buffer was flushed
				currentFileOffsetForWrites.set(0);
				maxIdForWrites.incrementAndGet();				
			}
						
			int size = buf.getInt();
			long off = bufferOffset.getAndAdd(size+4);
			if(off + size + 4 > bufferSize){
				// send current buffer to write queue
				ByteBuffer buff = activeBuffer.get();
				//verifyBuffer(buff);
				writeQueue.offer(buff);
				activeBuffer.set(null);
				
				if(currentFileLength + bufferSize > fileSizeLimit ){
					currentFileOffsetForWrites.set(0);
					maxIdForWrites.incrementAndGet();
				}
				// release lock
				writeLock.writeLock().unlock();
				// Get next buffer from empty queue
				ByteBuffer bbuf = emptyBuffersQueue.take();
				// lock again
				writeLock.writeLock().lock();
				if(activeBuffer.get() == null){
					activeBuffer.set(bbuf);
				} else {
					//  some other thread set already the activeBuffer
					// repeat call recursively
					emptyBuffersQueue.offer(bbuf);
					writeLock.writeLock().unlock();
					buf.position(pos);
					return storeData(buf);
				}
				bufferOffset.set(size + 4);
				// Check if need advance file
				
			}

			// We need to keep overall object (key+block) size in a file
			buf.position(pos);
			buf.limit(pos + size + 4);
			//LOG.info("pos="+pos+" capacity="+activeBuffer.get().capacity()+" size+4="+(size+4));
			activeBuffer.get().put(buf);
			
			FileStorageHandle fsh = new FileStorageHandle(maxIdForWrites.get(), (int)(currentFileOffsetForWrites.get()), size);
			// Increase offset in current file for writes;
			currentFileOffsetForWrites.addAndGet(size+4);
			return fsh;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			writeLock.writeLock().unlock();
			buf.position(pos);
			return storeData(buf);
			
		} finally{
			// TODO check if we have a lock
			WriteLock lock = writeLock.writeLock();
			if(lock.isHeldByCurrentThread()){
				lock.unlock();
			}
		}
	}

	/**
	 * Verify buffer.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
//	private void verifyBuffer(ByteBuffer buff) {
//		int current = buff.position();
//		
//		int limit = current > 0? current: buff.limit();
//
//		buff.position(0);
//		buff.limit(limit);
//		byte[] bytes = new byte[4];
//		try{
//			while(buff.hasRemaining()){
//				buff.get(bytes);
//				int size = Bytes.toInt(bytes);
//				if(size < 10000 || size > 11000){
//					LOG.fatal("ByteBuffer corrupted at "+(buff.position() -4) + "limit="+limit+ " current="+current+
//						" current file="+maxId.get()+" offset "+currentFileOffset.get()+" size="+size);
//					//buff.position(limit);
//					return;
//				} else{
//					int pos = buff.position();
//					buff.position(pos + size -4);
//				
//				}
//			}
//		} finally{
//			buff.position(0);
//			buff.limit(limit);
//		}
//
//		
//	}


	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.ExtStorage#close()
	 */
	@Override
	public void close() throws IOException {
		LOG.info("Closing storage {"+fileStorageBaseDir+"} ..." );		
		LOG.info("Stopping file flusher thread ...");		

		try{			
			if(flusher != null){										
				while(flusher.isAlive()){
					try {
						flusher.interrupt();
						flusher.join(10000);
						LOG.info("Expired 10000");
					} catch (InterruptedException e) {}								
				}
			}
			LOG.info("Flusher stopped.");
			// Flush internal buffer
			flush();
			writeLock.writeLock().lock();
			
			LOG.info("Closing open files ...");
			// Close all open files
			int count = 0;
			for(Queue<RandomAccessFile> files: readers.values())
			{
				for(RandomAccessFile f: files){
					try{
						count++;
						f.close();						
					} catch(Throwable t){
						
					}
				}
				files.clear();
			}
			
			readers.clear();
			if(currentForWrite != null){
				currentForWrite.close();
			}
			LOG.info("Closing open files ... Done {"+count+"} files.");
						
		} finally{
			writeLock.writeLock().unlock();
		}
		
		LOG.info("Closing storage {"+fileStorageBaseDir+"} ... Done." );
		
	}

	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.ExtStorage#flush()
	 */
	@Override
	public void flush() throws IOException {
//TODO this method flashes only internal buffer and does not touch internal flusher queue
		LOG.info("Flushing internal buffer to the storage" );
		long start = System.currentTimeMillis();
		writeLock.writeLock().lock();
		try{
			ByteBuffer buf = activeBuffer.get();
			if(bufferOffset.get() == 0) {
				// skip flush
				return;
			}
			if(buf != null){
				currentForWrite.getChannel().write(buf);
				buf.clear();
				bufferOffset.set(0);
				// we advance to next file;
				
				
			}
		} catch(Exception e){
			LOG.error(e);
		} finally{
			writeLock.writeLock().unlock();
		}
		LOG.info("Flushing completed in "+ (System.currentTimeMillis() - start)+"ms");
	}
	/**
	 * Gets the file storage base dir.
	 *
	 * @return the fileStorageBaseDir
	 */
	public String getFileStorageBaseDir() {
		return fileStorageBaseDir;
	}

	/**
	 * Gets the max storage size.
	 *
	 * @return the maxStorageSize
	 */
	public long getMaxStorageSize() {
		return maxStorageSize;
	}

	/**
	 * Gets the buffer size.
	 *
	 * @return the bufferSize
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	/**
	 * Gets the num buffers.
	 *
	 * @return the numBuffers
	 */
	public int getNumBuffers() {
		return numBuffers;
	}

	/**
	 * Gets the flush interval.
	 *
	 * @return the flushInterval
	 */
	public long getFlushInterval() {
		return flushInterval;
	}

	/**
	 * Gets the second chance fifo ratio.
	 *
	 * @return the secondChanceFIFORatio
	 */
	public float getSecondChanceFIFORatio() {
		return secondChanceFIFORatio;
	}

	/**
	 * Gets the file size limit.
	 *
	 * @return the fileSizeLimit
	 */
	public long getFileSizeLimit() {
		return fileSizeLimit;
	}

	/**
	 * Gets the max id.
	 *
	 * @return the maxId
	 */
	public AtomicInteger getMaxId() {
		return maxId;
	}

	/**
	 * Gets the min id.
	 *
	 * @return the minId
	 */
	public AtomicInteger getMinId() {
		return minId;
	}

	/**
	 * Gets the current storage size.
	 *
	 * @return the currentStorageSize
	 */
	public long getCurrentStorageSize() {
		return currentStorageSize.get();
//		long currentLength = 0;
//		try{
//			
//			currentLength = (currentForWrite != null )?currentForWrite.length(): 0;
//		}catch(IOException e){
//			//LOG.warn(e);
//		}
//		return currentStorageSize.get() + currentLength;
	}

	/**
	 * Gets the max open fd.
	 *
	 * @return the maxOpenFD
	 */
	public int getMaxOpenFD() {
		return maxOpenFD;
	}


	/**
	 * Gets the recycler.
	 *
	 * @return the recycler
	 */
	public StorageRecycler getRecycler() {
		return (StorageRecycler)recycler;
	}


	/**
	 * Gets the config.
	 *
	 * @return the config
	 */
	public Configuration getConfig() {
		return config;
	}


	/**
	 * Update storage size.
	 *
	 * @param delta the delta
	 */
	public void updateStorageSize(long delta) {

		currentStorageSize.addAndGet(delta);
		
	}

  @Override
  public long size() {
    long size = getCurrentStorageSize();
    long currentLength = 0;
	try{		
		currentLength = (currentForWrite != null )?currentForWrite.length(): 0;
	}catch(IOException e){
		//LOG.warn(e);
	}
	return size + currentLength;
    
  }

  @Override
  public void shutdown(boolean isPersistent) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public StorageHandle newStorageHandle() {
	  return new FileStorageHandle();
  }
}
