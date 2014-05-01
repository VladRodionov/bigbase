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
package com.koda.integ.hbase.test;

import java.io.File;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class TestUtils.
 */
public class TestUtils {

	/** The r. */
	static Random r = new Random();	
	
	/**
	 * Creates the byte array.
	 *
	 * @param size the size
	 * @return the byte[]
	 */
	public static byte[] createByteArray(int size)
	{
		byte[] array = new byte[size];
		r.nextBytes(array);
		Bytes.putInt(array, 0, size);
		return array;
	}
	
	
	/**
	 * Creates the byte arrays.
	 *
	 * @param size the size
	 * @param n the n
	 * @return the byte[][]
	 */
	public static byte[][] createByteArrays(int size, int n)
	{
		byte[][] arrays = new byte[n][];
		for(int i = 0; i < n; i++){
			arrays[i] = createByteArray(size);
		}
		return arrays;
	}
	/**
	 * Delete.
	 *
	 * @param f the f
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void delete(File f) throws IOException {
		  if (f.isDirectory()) {
		    for (File c : f.listFiles())
		      delete(c);
		  } else{
			  f.delete();
		  }

	}
	
  public static void appendToByteBuffer(final ByteBuffer bb, final KeyValue kv,
      final boolean includeMvccVersion) {
    // keep pushing the limit out. assume enough capacity
    bb.limit(bb.position() + kv.getLength());
    bb.put(kv.getBuffer(), kv.getOffset(), kv.getLength());
    // TODO tags
    if (includeMvccVersion) {
      int numMvccVersionBytes = WritableUtils.getVIntSize(kv.getMemstoreTS());
      bb.limit(bb.limit() + numMvccVersionBytes);
      ByteBufferUtils.writeVLong(bb, kv.getMemstoreTS());
    }
  }
}
