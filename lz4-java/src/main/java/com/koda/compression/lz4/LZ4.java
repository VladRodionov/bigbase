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
package com.koda.compression.lz4;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.koda.common.nativelib.LibLoader;
@SuppressWarnings("unused")

public class LZ4 {

	
	static{
	    LibLoader.loadNativeLibrary("lz4");
	}
	
	
	public static int compress(ByteBuffer src, ByteBuffer dst)
	{
		int len = src.remaining();
		int offset = src.position();
		int where = dst.position();
		int r = compressDirect(src, offset, len, dst, where+4);
		dst.putInt(where, len);
		dst.position(where);
		dst.limit(where + r + 4);
		return r+4;
	}
	
	public static int decompress(ByteBuffer src, ByteBuffer dst)
	{
		// First 4 bytes contains orig length
		int len = src.getInt();
		int offset = src.position();
		int where = dst.position();

    int r = decompressDirect(src, offset, len, dst, where);
		dst.position(where);
		dst.limit(where + len);
		return len;
	}
	
	 public static int compressHC(ByteBuffer src, ByteBuffer dst)
	  {
	    int len = src.remaining();
	    int offset = src.position();
	    int where = dst.position();
	    int r = compressDirectHC(src, offset, len, dst, where+4);
	    dst.putInt(where, len);
	    dst.position(where);
	    dst.limit(where + r + 4);
	    return r+4;
	  }
	  
	  public static int decompressHC(ByteBuffer src, ByteBuffer dst)
	  {
	    // First 4 bytes contains orig length
	    int len = src.getInt();
	    int offset = src.position();
	    int where = dst.position();

	    int r = decompressDirectHC(src, offset, len, dst, where);
	    dst.position(where);
	    dst.limit(where + len);
	    return len;
	  }
	
	/**
	 * 
	 * Compress block of data
	 * @param src    - source buffer (MUST BE DIRECT BUFFER)
	 * @param offset - offset in source
	 * @param len    - number of bytes to compress 
	 * @param dst    - destination buffer (MUST BE DIRECT BUFFER)  
	 * @param where  - offset in destination buffer
	 * @return       - total compressed bytes
	 */
	public static native int compressDirect(ByteBuffer src, int offset, int len, ByteBuffer dst, int where);
	
	public static native int compressDirectHC(ByteBuffer src, int offset, int len, ByteBuffer dst, int where);

	public static native int compressDirectAddress(long src, int offset, int len, long dst, int where);
	
	public static native int compressDirectAddressHC(long src, int offset, int len, long dst, int where);
	/**
	 *  Decompress block of data
	 * @param src    - source buffer
	 * @param offset - offset in source buffer
	 * @param dst    - destination buffer
	 * @param where  - offset  in destination buffer
	 * @return       - total decompressed bytes
	 */
	public static native int decompressDirect(ByteBuffer src, int offset, int origSize, ByteBuffer dst, int where);
	
	public static native int decompressDirectHC(ByteBuffer src, int offset, int origSize, ByteBuffer dst, int where);
  
	public static native int decompressDirectAddress(long src, int offset, int origSize, long dst, int where);
	
	public static native int decompressDirectAddressHC(long src, int offset, int origSize, long dst, int where);
	

	public static void main(String[] args)
	{
		String test = "TestTest12345678";
		
		int bufSize = 4096*1024;
		
		ByteBuffer src = ByteBuffer.allocateDirect(bufSize);
		src.order(ByteOrder.nativeOrder());
		ByteBuffer dst = ByteBuffer.allocateDirect(bufSize);
		dst.order(ByteOrder.nativeOrder());
//		byte[] buf = test.getBytes();
//	    int off = 0;
//		while(off < bufSize){
//		    src.put(buf);
//		    off += buf.length; 
//		}
//        int origSize = src.limit();		
//		int numIterations = 100000;
//        long start = System.currentTimeMillis();
//        src.flip();
//        for(int i=0; i < numIterations; i++){
//		    dst.clear();
//		    int compressedSize = compress(src, dst);		
//		//System.out.println("Original size="+origSize+" comp size="+compressedSize);		
//		    src.position(0);
//		    int r = decompress(dst, src);
//		}
//		long stop = System.currentTimeMillis();
//		
//		System.out.println((numIterations*1000)/(stop - start) +" of "+origSize +" blocks per sec");
//		
//        byte[] b = new byte[origSize];
//		System.out.println("src off="+src.position()+" src.limit="+src.limit());
//		src.get(b);
//		
//		System.out.println("Original     = "+test);
//		System.out.println("Decompressed = "+new String(b));
		
		String value = "value-value-value-value-value-value-value-value-value-value-value-value-value-value-value"+
		"value-value-value-value-value-value-value-value-value-value-value-value-value-value-value" +
		"value-value-value-value-value-value-value-value-value-value-value-value-value-value-value";
		int DST = 0;
		int SRC =0;
		src.clear();
		dst.clear();
		src.position(SRC);
		dst.position(DST);
		src.put(value.getBytes());
		int pos = src.position();
		src.position(SRC);
		src.limit(pos);
		
	    int compressedSize = compressHC(src, dst);		
		System.out.println("Original size="+value.length()+" comp size="+compressedSize);		
		
		for(int i = 0; i < compressedSize; i++){
			System.out.print(dst.get( DST + i)+" ");
		}
		System.out.println();
		src.position(SRC);
		dst.position(DST);
		dst.limit(DST + compressedSize);
		src.limit(src.capacity());
		System.out.println("src.pos="+dst.position()+" size="+dst.remaining()+" dst.pos="+src.position()+" limit="+src.limit());
		int r = decompressHC(dst, src);
		int size = src.limit() -SRC;
		byte[] v = new byte[size];
		src.position(SRC);
		src.get(v);
		String newValue = new String(v);
		System.out.println(value);
		System.out.println(newValue);
		System.out.println(newValue.equals(value));
		
	}
	
}
