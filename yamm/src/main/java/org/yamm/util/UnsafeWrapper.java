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
package org.yamm.util;
import sun.misc.Unsafe;

// TODO: Auto-generated Javadoc
/**
 * The Class UnsafeWrapper.
 */
public class UnsafeWrapper {

	/** The unsafe. */
	Unsafe unsafe;
	
	/**
	 * Instantiates a new unsafe wrapper.
	 *
	 * @param unsafe the unsafe
	 */
	public UnsafeWrapper(Unsafe unsafe)
	{
		this.unsafe = unsafe;
	}
	
	/**
	 * Allocate memory.
	 *
	 * @param size the size
	 * @return the long
	 */
	public long allocateMemory(long size)
	{
		long ptr = unsafe.allocateMemory(size);

		if(ptr == 0){
			System.err.println("FATAL: malloc failed");
		}
		if((ptr & 0xffffffffffffL) != ptr){
			System.err.println("PANIC huge VM address "+ ptr);
		}
		return ptr;
		
	}
	
	/**
	 * Put long.
	 *
	 * @param addr the addr
	 * @param v the v
	 */
	public void putLong(long addr, long v)
	{
		if(addr == 0L || addr > 0xffffffffffffL || addr < 0){
			System.err.println("FATAL: write "+ addr);
		}
		unsafe.putLong(addr, v);
		//IOUtils.putLong(addr, 0, v);
	}
	
	/**
	 * Sets the memory.
	 *
	 * @param mem the mem
	 * @param size the size
	 * @param v the v
	 */
	public void setMemory(long mem, long size, byte v)
	{
		unsafe.setMemory(mem, size, v);
	}
	
	/**
	 * Gets the long.
	 *
	 * @param addr the addr
	 * @return the long
	 */
	public long getLong(long addr){
		if(addr == 0L || addr > 0xffffffffffffL || addr < 0){
			System.err.println("FATAL: read "+ addr);
		}
		
		return unsafe.getLong(addr);
	}
	
	/**
	 * Free memory.
	 *
	 * @param ptr the ptr
	 */
	public void freeMemory(long ptr){
		if(ptr == 0){
			System.err.println("FATAL: free failed");
		}
		unsafe.freeMemory(ptr);
	}
	
	/**
	 * Copy memory.
	 *
	 * @param from the from
	 * @param to the to
	 * @param size the size
	 */
	public void copyMemory(long from, long to, long size){
		if(from == 0){
			System.err.println("FATAL: copy memory failed: from = 0x0");
		} else if( to == 0){
			System.err.println("FATAL: copy memory failed: to = 0x0");
		} else if (size <=0){
			System.err.println("FATAL: copy memory failed: size ="+size);
		}
		unsafe.copyMemory(from, to, size);
	}
}
