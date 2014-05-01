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

import org.yamm.util.Utils;

import com.koda.cache.OffHeapCache;
import com.koda.util.SpinLock;

// TODO: Auto-generated Javadoc
/**
 * The Class IOUtils.
 */
public class IOUtils {

	
	
	/**
	 * Gets the record size.
	 *
	 * @param ptr the ptr
	 * @return the record size
	 */
	public static int getRecordSize(long mptr)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			int ks = (int) getInt(ptr + OffHeapCache.OFFSET, 0);
			int vs = (int) getInt(ptr + OffHeapCache.OFFSET, 4);
			return ks + vs;
		} finally{
			NativeMemory.unlockAddress(mptr);
		}
	}
	/**
	 * Primitive values getter/setter.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the byte
	 */
	
	/**
	 * Atomic operations
	 */
	
	/**
	 * Atomic Byte
	 */
	public static byte incrementAndGetByte(long ptr, int off, byte inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			byte v = getByte(ptr, off);
			putByte(ptr, off, (byte)(v+inc));
			return (byte)(v+inc);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Gets the and increment byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the and increment byte
	 */
	public static byte getAndIncrementByte(long ptr, int off, byte inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			byte v = getByte(ptr, off);
			putByte(ptr, off, (byte)(v+inc));
			return (byte)(v);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Compare and swap byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param expectedVal the expected val
	 * @param newVal the new val
	 * @param lock the lock
	 * @return true, if successful
	 */
	public static boolean compareAndSwapByte(long ptr, int off, byte expectedVal, byte newVal, SpinLock lock)
	{
		while(!lock.lock());
		try{
			byte v = getByte(ptr, off);
			if(v == expectedVal){
				putByte(ptr, off, newVal);
				return true;
			}
			return false;
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Atomic UByte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the short
	 */

	public static short incrementAndGetUByte(long ptr, int off, short inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			short v = getUByte(ptr, off);
			putUByte(ptr, off, (short)(v+inc));
			return (short)(v+inc);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Gets the and increment u byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the and increment u byte
	 */
	public static short getAndIncrementUByte(long ptr, int off, short inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			short v = getUByte(ptr, off);
			putUByte(ptr, off, (short)(v+inc));
			return (short)(v);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Compare and swap u byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param expectedVal the expected val
	 * @param newVal the new val
	 * @param lock the lock
	 * @return true, if successful
	 */
	public static boolean compareAndSwapUByte(long ptr, int off, short expectedVal, short newVal, SpinLock lock)
	{
		while(!lock.lock());
		try{
			short v = getUByte(ptr, off);
			if(v == expectedVal){
				putUByte(ptr, off, newVal);
				return true;
			}
			return false;
		}finally{
			lock.unlock();
		}
	}

	
	/**
	 * Atomic Short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the short
	 */
	
	public static short incrementAndGetShort(long ptr, int off, short inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			short v = getShort(ptr, off);
			putShort(ptr, off, (short)(v+inc));
			return (short)(v+inc);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Gets the and increment short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the and increment short
	 */
	public static short getAndIncrementShort(long ptr, int off, short inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			short v = getShort(ptr, off);
			putShort(ptr, off, (short)(v+inc));
			return (short)(v);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Compare and swap short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param expectedVal the expected val
	 * @param newVal the new val
	 * @param lock the lock
	 * @return true, if successful
	 */
	public static boolean compareAndSwapShort(long ptr, int off, short expectedVal, short newVal, SpinLock lock)
	{
		while(!lock.lock());
		try{
			short v = getShort(ptr, off);
			if(v == expectedVal){
				putShort(ptr, off, newVal);
				return true;
			}
			return false;
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Atomic UShort.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the int
	 */
	
	public static int incrementAndGetUShort(long ptr, int off, int inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			int v = getUShort(ptr, off);
			putUShort(ptr, off, (int)(v+inc));
			return (int)(v+inc);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Gets the and increment u short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the and increment u short
	 */
	public static int getAndIncrementUShort(long ptr, int off, int inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			int v = getUShort(ptr, off);
			putUShort(ptr, off, (int)(v+inc));
			return (int)(v);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Compare and swap u short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param expectedVal the expected val
	 * @param newVal the new val
	 * @param lock the lock
	 * @return true, if successful
	 */
	public static boolean compareAndSwapUShort(long ptr, int off, int expectedVal, int newVal, SpinLock lock)
	{
		while(!lock.lock());
		try{
			int v = getUShort(ptr, off);
			if(v == expectedVal){
				putUShort(ptr, off, newVal);
				return true;
			}
			return false;
		}finally{
			lock.unlock();
		}
	}


	/**
	 * Atomic Int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the int
	 */
	
	public static int incrementAndGetInt(long ptr, int off, int inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			int v = getInt(ptr, off);
			putInt(ptr, off, (int)(v+inc));
			return (int)(v+inc);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Gets the and increment int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the and increment int
	 */
	public static int getAndIncrementInt(long ptr, int off, int inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			int v = getInt(ptr, off);
			putInt(ptr, off, (int)(v+inc));
			return (int)(v);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Compare and swap int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param expectedVal the expected val
	 * @param newVal the new val
	 * @param lock the lock
	 * @return true, if successful
	 */
	public static boolean compareAndSwapInt(long ptr, int off, int expectedVal, int newVal, SpinLock lock)
	{
		while(!lock.lock());
		try{
			int v = getInt(ptr, off);
			if(v == expectedVal){
				putInt(ptr, off, newVal);
				return true;
			}
			return false;
		}finally{
			lock.unlock();
		}
	}
	
	
	/**
	 * Atomic UInt.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the long
	 */
	
	public static long incrementAndGetUInt(long ptr, int off, long inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			long v = getUInt(ptr, off);
			putUInt(ptr, off, (long)(v+inc));
			return (long)(v+inc);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Gets the and increment u int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the and increment u int
	 */
	public static long getAndIncrementUInt(long ptr, int off, long inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			long v = getUInt(ptr, off);
			putUInt(ptr, off, (long)(v+inc));
			return (long)(v);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Compare and swap u int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param expectedVal the expected val
	 * @param newVal the new val
	 * @param lock the lock
	 * @return true, if successful
	 */
	public static boolean compareAndSwapUInt(long ptr, int off, long expectedVal, long newVal, SpinLock lock)
	{
		while(!lock.lock());
		try{
			long v = getUInt(ptr, off);
			if(v == expectedVal){
				putUInt(ptr, off, newVal);
				return true;
			}
			return false;
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Atomic Long.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the long
	 */
	
	public static long incrementAndGetLong(long ptr, int off, long inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			long v = getLong(ptr, off);
			putLong(ptr, off, (long)(v+inc));
			return (long)(v+inc);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Gets the and increment long.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param inc the inc
	 * @param lock the lock
	 * @return the and increment long
	 */
	public static long getAndIncrementLong(long ptr, int off, long inc, SpinLock lock)
	{
		while(!lock.lock());
		try{
			long v = getLong(ptr, off);
			putLong(ptr, off, (long)(v+inc));
			return (long)(v);
		}finally{
			lock.unlock();
		}
	}
	
	/**
	 * Compare and swap long.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param expectedVal the expected val
	 * @param newVal the new val
	 * @param lock the lock
	 * @return true, if successful
	 */
	public static boolean compareAndSwapLong(long ptr, int off, long expectedVal, long newVal, SpinLock lock)
	{
		while(!lock.lock());
		try{
			long v = getLong(ptr, off);
			if(v == expectedVal){
				putLong(ptr, off, newVal);
				return true;
			}
			return false;
		}finally{
			lock.unlock();
		}
	}	
	
	
	/**
	 * Gets the byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the byte
	 */
	public static byte getByte(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getByte(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}
	}
	
	/**
	 * Gets the u byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the u byte
	 */
	public static short getUByte(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getUByte(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}
	}
	
	/**
	 * Gets the short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the short
	 */
	public static short getShort(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getShort(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}
	}
	
	/**
	 * Gets the u short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the u short
	 */
	public static int getUShort(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getUShort(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}		
	}
	
	/**
	 * Gets the int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the int
	 */
	public static int getInt(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getInt(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}		
	}
	
	/**
	 * Gets the u int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the u int
	 */
	public static long getUInt(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getUInt(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}		
	}
	
	/**
	 * Gets the long.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the long
	 */
	public static long getLong(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getLong(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}		
	}
	
	/**
	 * Gets the float.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the float
	 */
	public static float getFloat(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getFloat(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}			
	}
	
	/**
	 * Gets the double.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @return the double
	 */
	public static double getDouble(long mptr, long off)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			return Utils.getDouble(ptr + off);
		} finally{
				NativeMemory.unlockAddress(mptr);
		}		
	}
	
	/**
	 * Put byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putByte(long mptr, long off, byte v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putByte(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}		
	}
	
	/**
	 * Put u byte.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putUByte(long mptr, long off, short v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putUByte(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}	
	}
	
	/**
	 * Put short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putShort(long mptr, long off, short v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putShort(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}	
	}
	
	/**
	 * Put u short.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putUShort(long mptr, long off, int v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putUShort(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}					
	}
	
	/**
	 * Put int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putInt(long mptr, long off, int v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putInt(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}		
	}
	
	/**
	 * Put u int.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putUInt(long mptr, long off, long v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putUInt(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}			
	}
	
	/**
	 * Put long.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putLong(long mptr, long off, long v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putLong(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}			
	}
	
	/**
	 * Put float.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putFloat(long mptr, long off, float v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putFloat(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}			
	}
	
	/**
	 * Put double.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param v the v
	 */
	public static void putDouble(long mptr, long off, double v)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.putDouble(ptr + off, v);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}			
	}
	
	/**
	 * Array operations.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the bytes
	 */
	public static void getBytes(long ptr, long off, byte[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(ptr + off, arr, arroff, size);
	}
	
	/**
	 * Gets the u bytes.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the u bytes
	 */
	public static void getUBytes(long ptr, long off, short[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(ptr + off, arr, arroff, size);
	}
	
	/**
	 * Gets the shorts.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the shorts
	 */
	public static void getShorts(long ptr, long off, short[] arr, int arroff, int size)
	{
		// TODO lock/unlock
		Utils.memcpy(ptr + off, arr, arroff, size);
	}
	
	/**
	 * Gets the u shorts.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the u shorts
	 */
	public static void getUShorts(long ptr, long off, int[] arr, int arroff, int size)
	{
		// TODO lock/unlock
		Utils.memcpy(ptr + off, arr, arroff, size);
	}
	
	/**
	 * Gets the ints.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the ints
	 */
	public static void getInts(long ptr, long off, int[] arr, int arroff, int size)
	{
		// TODO lock/unlock
		Utils.memcpy(ptr + off, arr, arroff, size);
	}
	
	/**
	 * Gets the u ints.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the u ints
	 */
	public static void getUInts(long ptr, long off, long[] arr, int arroff, int size)
	{
		// TODO lock/unlock
		Utils.memcpy(ptr + off, arr, arroff, size);
	}
	
	/**
	 * Gets the longs.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the longs
	 */
	public static void getLongs(long mptr, long off, long[] arr, int arroff, int size)
	{
		try{
			long ptr = NativeMemory.lockAddress(mptr);
			Utils.memcpy(ptr + off, arr, arroff, size);
		} finally{
			NativeMemory.unlockAddress(mptr);
		}	
	}
	
	/**
	 * Gets the floats.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the floats
	 */
//	public static void getFloats(long ptr, int off, float[] arr, int arroff, int size)
//	{
//		Utils.memcpy(ptr + off, arr, arroff, size);
//	}
	
	/**
	 * Gets the doubles.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 * @return the doubles
	 */
//	public static void getDoubles(long ptr, int off, double[] arr, int arroff, int size)
//	{
//		Utils.memcpy(ptr + off, arr, arroff, size);
//	}
	
	/**
	 * Put bytes.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
	public static void putBytes(long ptr, long off, byte[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(arr, arroff, size, ptr + off);
	}
	
	/**
	 * Put u bytes.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
	public static void putUBytes(long ptr, long off, short[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(arr, arroff, size, ptr + off);
	}
	
	/**
	 * Put shorts.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
	public static void putShorts(long ptr, long off, short[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(arr, arroff, size, ptr + off);
	}
	
	/**
	 * Put u shorts.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
	public static void putUShorts(long ptr, long off, int[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(arr, arroff, size, ptr + off);
	}
	
	/**
	 * Put ints.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
	public static void putInts(long ptr, long off, int[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(arr, arroff, size, ptr + off);
	}
	
	/**
	 * Put u ints.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
	public static void putUInts(long ptr, long off, long[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(arr, arroff, size, ptr + off);
	}
	
	/**
	 * Put longs.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
	public static void putLongs(long ptr, long off, long[] arr, int arroff, int size)
	{
		//TODO lock/unlock
		Utils.memcpy(arr, arroff, size, ptr + off);
	}
	
	/**
	 * Put floats.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
//	public static void putFloats(long ptr, int off, float[] arr, int arroff, int size)
//	{
//		//TODO lock/unlock
//		Utils.memcpy(arr, arroff, size, ptr + off);
//	}
	
	/**
	 * Put doubles.
	 *
	 * @param ptr the ptr
	 * @param off the off
	 * @param arr the arr
	 * @param arroff the arroff
	 * @param size the size
	 */
//	public static void putDoubles(long ptr, int off, double[] arr, int arroff, int size )
//	{
//		//TODO lock/unlock
//		Utils.memcpy(arr, arroff, size, ptr + off);
//	}
	
	
	/**
	 * TODO: IOUtils for Records
	 * TODO: Google fast regex
	 */
	
	//public static native int search(long memory, int limit, long key, int keySize);
	
}
