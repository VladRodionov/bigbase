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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public class LZ4Bench {

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    String fileName = args[0];
    
    File f = new File(fileName);
    System.out.println("Testing "+ args[0]);
    int fileSize = (int)f.length();
    
    ByteBuffer src = ByteBuffer.allocateDirect(fileSize);
    src.order(ByteOrder.nativeOrder());
    ByteBuffer dst = ByteBuffer.allocateDirect(fileSize);
    dst.order(ByteOrder.nativeOrder());
    
    FileInputStream fis = new FileInputStream(f);
    FileChannel channel = fis.getChannel();
    int read = 0;
    while( (read += channel.read(src)) < fileSize);
    
    System.out.println("Read "+read +" bytes");
    
    src.flip();
    long start = System.currentTimeMillis();
    int compressedSize = LZ4.compress(src, dst);
    long end = System.currentTimeMillis();
    System.out.println("Original size="+ fileSize +" comp size="+compressedSize+
        " Ratio ="+((double) fileSize/ compressedSize)+ " Time="+(end -start)+"ms");
  }

}
