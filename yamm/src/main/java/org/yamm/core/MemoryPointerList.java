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
package org.yamm.core;
import java.lang.reflect.Field;

//import com.koda.util.UnsafeAccess;


import sun.misc.Unsafe;

// TODO: Auto-generated Javadoc
/**
 * This class manages list of free memory pointers. It is similar to linked list
 *  but its totally off -heap. The overhead per memory allocation unit is 
 *  slightly greater than 8 bytes (pointer size in 64bit arch). The class is 
 *  not thread safe.
 * @author vrodionov
 *
 */
public class MemoryPointerList {

	/** The Constant PTR_SIZE. */
	private final static int PTR_SIZE = 8; // always 8 bytes
	
	/** The Constant unsafe. */
	private final static Unsafe unsafe ;//= UnsafeAccess.getUnsafe();
	
	/** The max block entries. */
	private int maxBlockEntries = 16*1024 -1; //(allocation is (maxIndex+1)* PTR_SIZE)
	//private int maxFreeBlocks = 2;
	/** Read access. */
	private long readBlockAddress;
	
	/** The read index. */
	private long  readIndex = -1;
	
	/** Write access. */
	private long writeBlockAddress;
	
	/** The write index. */
	private long writeIndex = -1;
	
	/** The total blocks. */
	private long totalBlocks;
	
	/** The size. */
	private long size;
	
    static
    {
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
    }
	
	/**
	 * Instantiates a new memory pointer list.
	 */
	public MemoryPointerList(){
		
	}
	
	/**
	 * Instantiates a new memory pointer list.
	 *
	 * @param maxBlockEntries the max block entries
	 */
	public MemoryPointerList(int maxBlockEntries){
		this.maxBlockEntries = maxBlockEntries;
		
	}
	
	/**
	 * Check allocation.
	 */
	private final void checkAllocation()
	{
		if(readBlockAddress == 0L){
			readBlockAddress = unsafe.allocateMemory(PTR_SIZE* (maxBlockEntries +1));
			writeBlockAddress = readBlockAddress;
			totalBlocks = 1;
		}
	}
	
	/**
	 * Returns first available free pointer, 0 - otherwise.
	 *
	 * @return the long
	 */
	public long get(){
		if(isEmpty()){
			return 0L;
		}else {
			if((readIndex < maxBlockEntries -1) || nextBlockRead()){
				return read();			
			}
			return 0L; 
		}
	}

	
	/**
	 * Returns first available free pointer, 0 - otherwise.
	 *
	 * @return the long
	 */
	public long peek(){
		if(isEmpty()){
			return 0L;
		}else {
			if((readIndex < maxBlockEntries -1) || nextBlockRead()){
				return peek0();			
			}
			return 0L; 
		}
	}
	
	/**
	 * Peek0.
	 *
	 * @return the long
	 */
	private long peek0() {

		if(readIndex == -1) readIndex++;
		
		long ptr = unsafe.getLong(readBlockAddress + PTR_SIZE * (readIndex));
		if(totalBlocks == 1 && readIndex == writeIndex ){
			readIndex = writeIndex = -1;
		}
		size--;
		return ptr;
	}
	/**
	 * Read.
	 *
	 * @return the long
	 */
	private long read() {
		// Increment index
		readIndex++;
		long ptr = unsafe.getLong(readBlockAddress + PTR_SIZE * (readIndex));
		if(totalBlocks == 1 && readIndex == writeIndex ){
			readIndex = writeIndex = -1;
		}
		size--;
		return ptr;
	}

	/**
	 * Next block read.
	 *
	 * @return true, if successful
	 */
	private boolean nextBlockRead() {

		long nextAddress = unsafe.getLong(readBlockAddress + maxBlockEntries * PTR_SIZE);
		if(nextAddress == 0L) {
			size =0;
			return false;
		}
		long freed = readBlockAddress;
		readBlockAddress = nextAddress;
		readIndex = -1;
		// Deallocate memory
		unsafe.freeMemory(freed);
		// Decrement total blocks
		totalBlocks --;
		return true;		
	}

	/**
	 * Checks if is empty.
	 *
	 * @return true, if is empty
	 */
	public final boolean isEmpty()
	{
		return size ==0;//(readBlockAddress == writeBlockAddress && readIndex == writeIndex) /*|| readIndex < 0*/;
	}
	
	/**
	 * Put new pointer into the buffer (list).
	 *
	 * @param ptr the ptr
	 */
	public void put(long ptr)
	{
		checkAllocation();
		// check if we reached the end of current block
		if( (writeIndex < maxBlockEntries -1) || nextBlockWrite()){
			write(ptr);
		}
	}

	/**
	 * Write.
	 *
	 * @param ptr the ptr
	 */
	private void write(long ptr) {
		writeIndex++;
		unsafe.putLong(writeBlockAddress + PTR_SIZE * writeIndex, ptr);
		size++;
		
	}

	/**
	 * Next block write.
	 *
	 * @return true, if successful
	 */
	private boolean nextBlockWrite() {

		long current = writeBlockAddress;
		long newalloc = unsafe.allocateMemory((maxBlockEntries +1)* PTR_SIZE);
		unsafe.putLong(current + maxBlockEntries * PTR_SIZE, newalloc);
		// 0fy ptr to next block
		unsafe.putLong(newalloc + maxBlockEntries * PTR_SIZE, 0L);
		writeBlockAddress = newalloc;
		writeIndex = -1;
		// always returns true
		totalBlocks++;
		return true;
	}
	
	/**
	 * Total blocks.
	 *
	 * @return the long
	 */
	public long totalBlocks()
	{
		return totalBlocks;
	}
	
	/**
	 * Block size.
	 *
	 * @return the long
	 */
	public long blockSize()
	{
		return (maxBlockEntries + 1) * PTR_SIZE;
	}
	
	/**
	 * Memory allocated.
	 *
	 * @return the long
	 */
	public long memoryAllocated(){
		return totalBlocks*(maxBlockEntries + 1) * PTR_SIZE; 
	}
	
	/**
	 * TODO - fix this method.
	 *
	 * @return the long
	 */
	public long size()
	{
		return size;//(totalBlocks -2)* maxBlockEntries + (maxBlockEntries - readIndex) + writeIndex ;
	}
	
	/**
	 * Max block entries.
	 *
	 * @return the int
	 */
	public int maxBlockEntries()
	{
		return maxBlockEntries;
	}
	
	/**
	 * Dump.
	 */
	public void dump(){
		System.out.println("empty="+isEmpty()+" size="+size);
	}
}
