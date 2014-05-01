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
package com.koda.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class Utils {

  public static String concat(Object[] arr, String delim) {
    StringBuffer sb = new StringBuffer();

    for (int i = 0; i < arr.length; i++) {
      sb.append(arr[i]);
      if (i < arr.length - 1) {
        sb.append(delim);
      }
    }
    return sb.toString();

  }

  public static boolean equals(byte[] b1, byte[] b2) {
    if (b1 == null || b2 == null)
      return false;
    for (int i = 0; i < b1.length; i++) {
      if (b1[i] != b2[i])
        return false;
    }
    return true;
  }

  // Returns an output stream for a ByteBuffer.
  // The write() methods use the relative ByteBuffer put() methods.
  public static OutputStream newOutputStream(final ByteBuffer buf) {
    return new OutputStream() {
      public synchronized void write(int b) throws IOException {
        buf.put((byte) b);
      }

      public synchronized void write(byte[] bytes, int off, int len)
          throws IOException {
        buf.put(bytes, off, len);
      }
    };
  }

  // Returns an input stream for a ByteBuffer.
  // The read() methods use the relative ByteBuffer get() methods.
  public static InputStream newInputStream(final ByteBuffer buf) {
    return new InputStream() {
      private int mark = -1;
      
      @Override
      public boolean markSupported() {
        //System.out.println("markSupported ");

        return true;
      }
      
      @Override
      public void mark(int readLimit){
        //System.out.println("mark "+readLimit);
        mark = buf.position();
      }
      
      
      @Override      
      public void reset(){
        if(mark >=0){
          //System.out.println("reset to "+ mark);

          buf.position(mark);
          mark = -1;
        }
      }
      
      public synchronized int read() throws IOException {
        //System.out.println("read ");

        if (!buf.hasRemaining()) {
          return -1;
        }
        return buf.get();
      }

      public synchronized int read(byte[] bytes, int off, int len)
          throws IOException {
        // Read only what's left
        //System.out.println("read "+ len+" off="+off+" array.length="+bytes.length+" rem="+buf.remaining());
        if( buf.remaining() == 0) return -1;
        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        
        return len;
      }
      
      @Override
      public int available()
      {
        //System.out.println("available "+buf.remaining());

        return buf.remaining();
      }
      
      @Override
      public long skip(long v){
        //System.out.println("skip "+v);

        if( v > 0) {
          v = Math.min(v, buf.remaining());
        } else{
          //v = Math.max( - buf.position(), v); 
          return 0;
        }
        
        int off = buf.position();
        buf.position(off + (int)v);
        return v;
      }
    };
  }
}
