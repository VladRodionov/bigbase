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
package com.koda.cache;

import sun.misc.Unsafe;

import com.koda.IOUtils;
import com.koda.NativeMemory;

// TODO: Auto-generated Javadoc
/**
 * The Class AbstractScannerImpl.
 */
public abstract class AbstractScannerImpl implements Scanner {

	
	/**
	 * The Enum Prefetch.
	 */
	public static enum Prefetch{
		
		/** The READ. */
			READ, 
		/** The WRITE. */
			WRITE
	}
	
	/**
	 * The Enum PrefetchLocality.
	 */
	public static enum PrefetchLocality{
		
		/** The LOW. */
			LOW, 
		/** The MEDIUM. */
			MEDIUM, 
		/** The HIGH. */
			HIGH, 
		/** The VER y_ high. */
			VERY_HIGH
	}
	/** The m prefetch interval. */
	protected int mPrefetchInterval = 1;// Default
    
	/** The m prefetch lines. */
	protected int mPrefetchLines = 3; // Default
    
    /** The m prefetch enabled. */
    protected boolean mPrefetchEnabled = true;
    
    /** The m prefetch type. */
    protected Prefetch mPrefetchType = Prefetch.READ;
    
    /** The m locality. */
    protected PrefetchLocality mLocality = PrefetchLocality.LOW;
	
	/**
	 * Sets the prefetch enabled.
	 *
	 * @param value the new prefetch enabled
	 */
	public void setPrefetchEnabled(boolean value)
	{
		this.mPrefetchEnabled = value;
	}
	
	/**
	 * Checks if is prefetch enabled.
	 *
	 * @return true, if is prefetch enabled
	 */
	public boolean isPrefetchEnabled()
	{
		return mPrefetchEnabled;
	}
	
	/**
	 * Sets the prefetch type.
	 *
	 * @param type the new prefetch type
	 */
	public void setPrefetchType(Prefetch type)
	{
		this.mPrefetchType = type;
	}
	
	/**
	 * Gets the prefetch type.
	 *
	 * @return the prefetch type
	 */
	public Prefetch getPrefetchType()
	{
		return mPrefetchType;
	}
	
	/**
	 * Sets the prefetch locality.
	 *
	 * @param locality the new prefetch locality
	 */
	public void setPrefetchLocality(PrefetchLocality locality)
	{
		this.mLocality = locality;
	}
	
	/**
	 * Gets the prefetch locality.
	 *
	 * @return the prefetch locality
	 */
	public PrefetchLocality getPrefetchLocality()
	{
		return mLocality;
	}
	
	/**
	 * Sets the prefetch cache lines.
	 *
	 * @param lines the new prefetch cache lines
	 */
	public void setPrefetchCacheLines(int lines)
	{
		this.mPrefetchLines = lines;
	}
	
	/**
	 * Gets the prefetch cache lines.
	 *
	 * @return the prefetch cache lines
	 */
	public int getPrefetchCacheLines()
	{
		return mPrefetchLines;
	}
	/**
	 * Scan fill - native method.
	 *
	 * @param buffer the buffer
	 * @param start the start
	 * @param end the end
	 * @param array the array
	 * @return the int
	 */
	protected static long scanFill(long bufPtr, long start, long end, long[] array)
	{
		//System.out.println("scanFill "+start +" "+ end);
		long bufptr = NativeMemory.lockAddress(bufPtr);
		// long bufptr = NativeMemory.getBufferAddress(buffer);
		Unsafe unsafe = NativeMemory.getUnsafe();

		try {

			final long[] arrptr = array;
			final int arrlen = array.length;
			long index = start;
			int idx = 0, i = 0;
			long ptr;

			for (; index <= end; index++) {

				if (idx == arrlen) {
					index--;
					break;
				}

				if (unsafe.getLong(bufptr + index * 8) == 0L)
					continue;

				arrptr[idx++] = unsafe.getLong(bufptr + (index) * 8) & 0x7fffffffffffffffL;

				if ((unsafe.getLong(bufptr + (index) * 8) & 0x8000000000000000L) == 0)
					continue;

				i = 1;

				ptr = unsafe.getLong(bufptr + (index) * 8) & 0x7fffffffffffffffL;

				do {

					ptr = IOUtils.getLong((ptr & 0x7fffffffffffffffL),
							OffHeapCache.OFFSET - 8);
					if (idx == arrlen) {

						if (ptr != 0) {
							// 0s last i+1
							for (; i > 0; i--) {
								arrptr[idx - i] = 0;
							}
							index--;
						}

						break;
					}

					if (ptr != 0) {
						arrptr[idx++] = ptr & 0x7fffffffffffffffL;
					}
					i++;
				} while ((ptr & 0x8000000000000000L) != 0);

			}

			if (idx < arrlen){
				arrptr[idx] = 0;
			}
			if (index > end){
				index = end;
			}
			//System.out.println("return "+index);
			return index;
		} finally {
			NativeMemory.unlockAddress(bufPtr);
		}

	}

	/**
	 * Prefetch.
	 *
	 * @param ptr the ptr
	 * @param mode the mode
	 * @param locality the locality
	 * @param cacheLines the cache lines
	 */
	public static void prefetch(long ptr, int mode, int locality, int cacheLines)
	{
		// does nothing
	}
}
