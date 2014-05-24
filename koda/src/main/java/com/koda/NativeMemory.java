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
package com.koda;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.yamm.core.ConcurrentMalloc;
import org.yamm.core.Malloc;
import org.yamm.core.MallocNoCompactionFactory;
//import org.yamm.core.MallocWithCompactionFactory;
import org.yamm.util.Utils;

import sun.misc.Unsafe;

import com.koda.config.CacheConfiguration;

/**
 * 
 * OpenJDK does not support Unsafe.copyMemory(Ljava.lang.Object ...)
 * http://netty.io/4.0/xref/io/netty/util/internal/PlatformDependent0.html
 * 
 * Need to patch sun.misc.Unsafe?
 */

// TODO: Auto-generated Javadoc
/**
 * The Class NativeMemory.
 */
public class NativeMemory {

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(NativeMemory.class);	
	
	/** Default Platform serializer. */
	private final static long[] slabSizes = new long[]{
	  //32,
	  //40,
	  //48,
	  64,
	  72,
	  88,
	  104,
	  120,
	  144,
	  168,
	  200,
	  232,
	  272,
	  320,
	  376,
	  440,
	  512,
	  592,
	  688,
	  792,
	  912,
	  1056,
	  1216,
	  1400,
	  1616,
	  1864,
	  2144,
	  2472,
	  2848,
	  3280,
	  3776,
	  4344,
	  5000,
	  5752,
	  6616,
	  7616,
	  8760,
	  10080,
	  11600,
	  13344,
	  15352,
	  17656,
	  20312,
	  23360,
	  26864,
	  30896,
	  35536,
	  40872,
	  47008,
	  56416,
	  67704,
	  81248,
	  97504,
	  117008,
	  140416,
	  168504,
	  210632,
	  263296,
	  329128,
	  411416,
	  514272,
	  642848,
	  803568,
	  1004464, // LAST one ~ 1M we support in real allocation
	  1155136,
	  1328408,
	  1527672,
	  1756824,
	  2020352,
	  2323408,
	  2671920,
	  3072712,
	  3533624,
	  4063672,
	  4673224,
	  5374208,
	  6180344,
	  7107400,
	  8173512,
	  9399544,
	  10809480,
	  12430904,
	  14295544,
	  16439880,
	  18905864,
	  21741744,
	  25003008,
	  28753464,
	  33066488,
	  38026464,
	  43730440,
	  50290008,
	  57833512,
	  66508544,
	  76484832,
	  87957560,
	  101151200,
	  116323880,
	  133772464,
	  153838336,
	  176914088,
	  203451208,
	  233968896,
	  269064232,
	  309423872,
	  355837456,
	  409213080,
	  470595048,
	  541184312,
	  622361960,
	  715716256,
	  823073696,
	  946534752,
	  1088514968,
	  1251792216,
	  1439561056,
	  1655495216,
	  1903819504,
	  2189392432L,
	  2517801304L,
	  2895471504L,
	  3329792232L,
	  3829261072L,
	  4403650240L,
	  5064197784L,
	  5823827456L,
	  6697401576L,
	  7702011816L,
	  7702011816L,
	  7702011816L,
	  7702011816L

	  
	};
	
	private static int concurrencyLevel = System.getProperty(CacheConfiguration.MALLOC_CONCURRENCY) != null?
           Integer.parseInt(System.getProperty(CacheConfiguration.MALLOC_CONCURRENCY)): Runtime.getRuntime().availableProcessors() /2;
	
	/** Memory Allocator*/
	private static Malloc malloc =  
		 new ConcurrentMalloc(MallocNoCompactionFactory.getInstance(), concurrencyLevel);
     //new ConcurrentMalloc(MallocWithCompactionFactory.getInstance(), concurrencyLevel);

	static{
	  // set custom slab sizes
	  // up to 47KB we move with 1.15 exp factor
	  // up to 200KB we have 1.2
	  // between 200K and 1M - 1.25
	  // after 1M - 1.15 again (not because we are  but to make sure we)
	  // FIXME: uncomment
	  malloc.setSlabSizes(slabSizes);	  
	}
	
	/** Unsafe */
	private static Unsafe unsafe ;
	/** The memory. */
	private static AtomicLong memory = new AtomicLong(0);
	
	/** The last time stat. */
	private static long lastTimeStat = System.currentTimeMillis();
	
	/** The stat interval. */
	private static long statInterval = 5000;
		
	private static boolean mallocDebug = System.getProperty(CacheConfiguration.MALLOC_DEBUG) != null?
                    Boolean.parseBoolean(System.getProperty(CacheConfiguration.MALLOC_DEBUG)): false;                                           
	
    private static boolean memopDebug = System.getProperty(CacheConfiguration.MEMOPS_DEBUG) != null?
                    Boolean.parseBoolean(System.getProperty(CacheConfiguration.MEMOPS_DEBUG)): false;
    static{
        try
        {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe)field.get(null);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
	    //LOG.info("[NativeMemory] malloc concurrent ="+mallocConcurrent);
	    LOG.info("[NativeMemory] malloc debug      ="+mallocDebug);
	    LOG.info("[NativeMemory] memops debug      ="+memopDebug);
	}
	                                            
	
    public static void setMalloc(Malloc m){
    	malloc = m;
    }
    
    public static Malloc getMalloc()
    {
    	return malloc;
    }
    
    public static Unsafe getUnsafe()
    {
    	return unsafe;
    }
    
    public static long lockAddress(long ptr)
    {
    	if( (ptr & 0xffffffffffffL) == ptr) return ptr;    	
    		return malloc.lockAddress(ptr);
    }
    
    public static long realAddress(long ptr)
    {
    	if( (ptr & 0xffffffffffffL) == ptr) return ptr; 
    	return malloc.realAddress(ptr);
    }
    
    public static void unlockAddress(long ptr)
    {
    	if( (ptr & 0xffffffffffffL) != ptr){
    		malloc.unlockAddress(ptr);
    	}
    }
    
    public static void mallocDebug(boolean value)
    {
        mallocDebug = value;
    }
    
    public static boolean isMallocDebug()
    {
        return mallocDebug;
    }
    
    public static void memopDebug(boolean value)
    {
        memopDebug = value;
    }
    
    public static boolean isMemopDebug()
    {
        return memopDebug;
    }  
    
	
	
	
	/**
	 * Malloc.
	 *
	 * @param size the size
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static long malloc(long size) 
	{

		
		//if(!mallocConcurrent) while(!globalLock.lock());
		
		try{
			long ptr = 0L;
			int attempt = 0;
			//FIXME: uncomment
			int maxAttempts = ((ConcurrentMalloc)malloc).getConcurrencyLevel();
			// We iterate over all allocators
			while( (ptr = malloc.malloc(size)) == 0L &&  attempt++ < maxAttempts);
			// Allocation failed
			if( ptr == 0L) return 0L;
      
			if(mallocDebug ){
			    LOG.info("[malloc.malloc]: "+ptr+" size="+mallocUsableSize(ptr));
			}
			memory.addAndGet(mallocUsableSize(ptr));
			//stats();
			return ptr; 
		}finally{
		    //if(!mallocConcurrent) globalLock.unlock();
		}
	}
	
	/**
	 * Stats.
	 */
	@SuppressWarnings("unused")
    private static void stats()
	{
		if(System.currentTimeMillis() - lastTimeStat > statInterval){
			lastTimeStat = System.currentTimeMillis();
			LOG.info("Memory="+memory.get());
		}
	}
	
	
	/**
	 * Posix_memalign.
	 *
	 * @param alignment the alignment
	 * @param size the size
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static long posix_memalign(int alignment, int size) 
	{
		//TODO memory aligned allocation is not supported yet
		return malloc(size); 
	}
	
	public static long getTotalAllocatedMemory()
	{
	    return memory.get();
	}
	

	
	/**
	 * Realloc.
	 *
	 * @param ptr the ptr
	 * @param newSize the new size
	 * @return the long
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static long realloc(long ptr, int newSize) 
	{

		
	    //if(!mallocConcurrent) while(!globalLock.lock());
		
		try{
			long oldSize = mallocUsableSize(ptr);
			long newPtr = malloc.ralloc(ptr,newSize);
			if( newPtr == 0L) return 0L;
			long allocd = mallocUsableSize(newPtr);
			memory.addAndGet(allocd - oldSize);
	        if(mallocDebug){
	                LOG.info("[malloc.realloc]: "+ptr+" size="+(allocd - oldSize));
	        }
			//stats();
			return newPtr;
		}finally{
		    //if(!mallocConcurrent) globalLock.unlock();
		}
	}
	
	

	
	/**
	 * Free.
	 *
	 * @param ptr the ptr
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static void free(long ptr) 
	{
		
	    //if(!mallocConcurrent) while(!globalLock.lock());
		long freed = mallocUsableSize(ptr);
		try{
		    if(mallocDebug) LOG.info("[malloc.free]: "+ptr+" size="+freed); 
		    malloc.free(ptr);
			memory.addAndGet(-freed);
		}finally{
		   // if(!mallocConcurrent) globalLock.unlock();
		}
	}
	
	
	/**
	 * Allocate direct buffer.
	 *
	 * @param align the align
	 * @param size the size
	 * @return the byte buffer
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static ByteBuffer allocateDirectBuffer(int align, int size) 
	{
	   // if(!mallocConcurrent) while(!globalLock.lock());
		
		try{
			if(mallocDebug){
			    LOG.info("[malloc: allocateDirectBuffer] size="+size);
			}
		    ByteBuffer buf = ByteBuffer.allocateDirect(size);
			buf.order(ByteOrder.nativeOrder());
			return buf;
		}finally{
		   // if(!mallocConcurrent) globalLock.unlock();
		}	
		
	}
	
	/**
	 * Reallocate direct buffer.
	 *
	 * @param align the align
	 * @param size the size
	 * @param ptr the ptr
	 * @return the byte buffer
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static ByteBuffer reallocateDirectBuffer(int align, int size, long ptr) 
	{
		return allocateDirectBuffer(align, size);
	}
	
	/**
	 * Memcpy from Off Heap to Java.
	 *
	 * @param ptr the ptr
	 * @param ptroff native offset
	 * @param buffer the buffer
	 * @param off the off
	 * @param size the size
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static void memcpy(long mptr, int ptroff , byte[] buffer, int off, int size) 
	{
		//TODO lock/unlock (not used yet)
		long ptr = lockAddress(mptr);		
		Utils.memcpy(ptr + ptroff, buffer, off, size);
		unlockAddress(mptr);
	}
			

	
	public static void memcpy(byte[] buffer, int off, int size, long mptr, int ptroff) 
	{
		//TODO lock/unlock (not used yet)	
		long ptr = lockAddress(mptr);
		Utils.memcpy(buffer, off, size, ptr + ptroff);
		unlockAddress(mptr);
	}
	
	private static int counter =0;
	
	public static void memcpyDirect(long from, long fromOffset, long to, long toOffset, int size)
	{
		//TODO lock/unlock (not used yet)

            if(memopDebug){
                counter++;
                long fromAllocd = NativeMemory.mallocUsableSize(from);
                if(fromOffset + size > fromAllocd) throw new RuntimeException("memory corruption");
                if(fromOffset + size < 0) throw new RuntimeException("memory corruption");
                long toAllocd = NativeMemory.mallocUsableSize(to);
                if(toOffset + size > toAllocd){
                        LOG.error("from       ="+from);
                        LOG.error("fromOffset ="+fromOffset);                
                        LOG.error("to         ="+to);
                        LOG.error("toOffset   ="+toOffset);
                        LOG.error("fromAllocd ="+ fromAllocd);
                        LOG.error("toAllocd   ="+ toAllocd);
                        LOG.error("size       ="+ size);
                
                
                        throw new RuntimeException("memory corruption");
                }
                if(toOffset + size < 0) throw new RuntimeException("memory corruption");
            }
            unsafe.copyMemory(from+fromOffset, to + toOffset, size);
	}
	
	public static void memcpy(long src, long dst, long size){
		unsafe.copyMemory(src, dst, size);
	}

	
	/**
	 * Memcpy from Off Heap to Java Heap.
	 *
	 * @param ptr the ptr
	 * @param ptroff - native offset
	 * @param buffer the buffer
	 * @param off the off
	 * @param size the size
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static void memcpy(long mptr, int ptroff, ByteBuffer buffer, int off, int size)
	{
		long ptr = NativeMemory.lockAddress(mptr);
		
		Utils.memcpy(ptr + ptroff, buffer, off, size);
		NativeMemory.unlockAddress(mptr);
	}
	
	/**
	 * Memcpy from Java Heap to Off Heap memory.
	 *
	 * @param buffer the buffer
	 * @param off the off
	 * @param size the size
	 * @param ptr the ptr
	 * @param ptroff - native offset
	 * @throws NativeMemoryException the jemalloc exception
	 */
	public static  void memcpy(ByteBuffer buffer, int off, int size, long mptr, int ptroff) 
	{
		long ptr = NativeMemory.lockAddress(mptr);
		Utils.memcpy(buffer, off, size, ptr + ptroff);
		NativeMemory.unlockAddress(mptr);
	}
	
	
	
	
	
	/**
	 * Memread to existing array.
	 *
	 * @param ptr the ptr
	 * @param offset the offset
	 * @param size the size
	 * @param value the value
	 * @return the byte buffer
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static byte[] memread2ea(long mptr, int offset, int size, byte[] value) 
	{
		long ptr = NativeMemory.lockAddress(mptr);
		Utils.memcpy(ptr + offset, value, 0, size);
		NativeMemory.unlockAddress(mptr);
		return value;
	}
	

	

	
	/**
	 * Memread2s.
	 *
	 * @param ptr the ptr
	 * @param offset the offset
	 * @param size the size
	 * @return the string
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static  String memread2s(long mptr) {
		long ptr = NativeMemory.lockAddress(mptr);
		String s = Utils.readString(ptr);
		NativeMemory.unlockAddress(mptr);
		return s;
	}

	
	/**
	 * Memcpy.
	 *
	 * @param s the s
	 * @param off the off
	 * @param size the size
	 * @param ptr the ptr
	 * @param ptroff the ptroff
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static void memcpy(String s, int off, int size, long mptr, int ptroff) 
	{
		long ptr = NativeMemory.lockAddress(mptr);
		Utils.memcpy(s, off, size, ptr + ptroff);
		NativeMemory.unlockAddress(mptr);
	}
	

	public static long memcmp(long src, long dst, int size) {
		final int num_4 = size >>> 2;
		final long end = src + num_4 * 4;
	    final long stop = src + size;
		while( src < end){
			final long v1 = unsafe.getInt(src) & 0xffffffffL;
			final long v2 = unsafe.getInt(dst) & 0xffffffffL;
	    	if( v1 != v2){
	    		return v1 - v2;
	    	}
	    	src +=4; dst +=4;
	    }
	    
	    while( src < stop){
	    	final short s1 = (short)(unsafe.getByte(src++) & 0xff);
	    	final short s2 = (short)(unsafe.getByte(dst++) & 0xff);
	    	if(s1 != s2) return s1 -s2;
	    	
	    }
		return 0;
	}		
	
	/**
	 * Gets the buffer address.
	 *
	 * @param buf the buf
	 * @return the buffer address
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static long getBufferAddress(ByteBuffer buf) 
	{
		return Utils.getBufferAddress(buf);
	}
	
	public static long getBufferAddress(LongBuffer buf) 
	{
		return Utils.getBufferAddress(buf);
	}
	
	/**
	 * Malloc usable size.
	 *
	 * @param ptr the ptr
	 * @return the int
	 * @throws NativeMemoryException the j emalloc exception
	 */
	public static long mallocUsableSize(long ptr) {
		return (long) malloc.mallocUsableSize(ptr);
	}

	public static void memmove(long toBlock, int offset, long toBlock2, int i,
			int j) {
		// TODO Auto-generated method stub
		
	}

	public static void memmove(long from, long to, int i) {
		// TODO Auto-generated method stub
		
	}
	

	
}
