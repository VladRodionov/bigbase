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
package com.koda.integ.hbase.util;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;


// TODO: Auto-generated Javadoc
/**
 * The Class Utils.
 */
public class Utils {

	/**
	 * To long.
	 *
	 * @param bytes the bytes
	 * @param offset the offset
	 * @return the long
	 */
	public static long toLong(byte[] bytes, int offset) {
		long l = 0;
		for (int i = offset; i < offset + 8; i++) {
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}

	/**
	 * To int.
	 *
	 * @param bytes the bytes
	 * @param offset the offset
	 * @return the int
	 */
	public static int toInt(byte[] bytes, int offset) {
		int l = 0;
		for (int i = offset; i < offset + 4; i++) {
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}

	/**
	 * Convert a long value to a byte array using big-endian.
	 *
	 * @param b the b
	 * @param offset the offset
	 * @param val value to convert
	 * @return the byte array
	 */
	public static byte[] toBytes(byte[] b, int offset, long val) {

		for (int i = offset + 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[offset] = (byte) val;
		return b;
	}

	/**
	 * To bytes.
	 *
	 * @param b the b
	 * @param offset the offset
	 * @param val the val
	 * @return the byte[]
	 */
	public static byte[] toBytes(byte[] b, int offset, int val) {

		for (int i = offset + 3; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[offset] = (byte) val;
		return b;
	}
	
	/**
	 * Hash128.
	 *
	 * @param s the s
	 * @return the byte[]
	 */
	public static byte[] hash128(String s){
		int h1 = com.koda.util.Utils.hashString(s, 123);
		int h2 = com.koda.util.Utils.hashString(s, 124);
		int h3 = com.koda.util.Utils.hashString(s, 125);
		int h4 = com.koda.util.Utils.hashString(s, 126);
		byte[] hash = new byte[16];
		Bytes.putInt(hash, 0, h1);
		Bytes.putInt(hash, 4, h2);
		Bytes.putInt(hash, 8, h3);
		Bytes.putInt(hash, 12, h4);
		return hash;
	}
	
	/**
	 * Hash128.
	 *
	 * @param bytes the bytes
	 * @return the byte[]
	 */
	public static byte[] hash128(byte[] bytes)
	{
		int h1 = com.koda.util.Utils.hash(bytes, 123);
		int h2 = com.koda.util.Utils.hash(bytes, 124);
		int h3 = com.koda.util.Utils.hash(bytes, 125);
		int h4 = com.koda.util.Utils.hash(bytes, 126);
		byte[] hash = new byte[16];
		Bytes.putInt(hash, 0, h1);
		Bytes.putInt(hash, 4, h2);
		Bytes.putInt(hash, 8, h3);
		Bytes.putInt(hash, 12, h4);
		return hash;
	}
	
	/**
	 * No array length
	 * @param buf
	 * @param arr
	 */
	public static int[] readIntArray(ByteBuffer buf)
	{
	  final int size = buf.getInt();
	  int[] arr = new int[size];
	  for(int i=0; i < size; i++){
	    arr[i] = buf.getInt();
	  }
	  return arr;
	}
	
	public static void writeIntArray(ByteBuffer buf, int[] arr)
	{
	  final int size = arr.length;
	  buf.putInt(size);
	  for(int i = 0; i < size; i++)
	  {
	     buf.putInt(arr[i]); 
	  }	  
	}
}
