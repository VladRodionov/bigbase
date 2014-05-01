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
package com.koda.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.log4j.Logger;
import org.yamm.util.Utils;

import com.koda.util.CLib;
import com.koda.util.CLib.LibC;
import com.sun.jna.Pointer;
@SuppressWarnings("unused")

public class CLIBTest {
  private final static Logger LOG = Logger
  .getLogger(CLIBTest.class);
  
  static final int BLOCK_SIZE = 4096;
  static byte[] data = new byte[BLOCK_SIZE];
  static int fileSizeInBlocks = 100000; // in blocks
  static int fd;
  static{
    Random r = new Random();
    r.nextBytes(data);
  }
  
  static File file;
  
  public static void main(String[] args) throws IOException
  {
    file = File.createTempFile("test", ".tmp");
    file.deleteOnExit();
            
    LOG.info("Created "+file.getAbsolutePath());
    
    writeData();
    
    LOG.info("Filled "+file.getAbsolutePath());
    
    testNativeRead();
    testDirectRead();
    
  }

  private static void testNativeRead() throws IOException {
    
    LOG.info("Reading file, using native 'read' API");
    ByteBuffer buf = ByteBuffer.allocateDirect(2 * BLOCK_SIZE);
    long address = Utils.getBufferAddress(buf);
    Pointer p = new Pointer(address);
    LibC libc = CLib.LIBC;
    FileInputStream fis =  new FileInputStream(file);
    fd = CLib.getfd(fis.getFD());
    long start = System.currentTimeMillis();
    for( int i=0; i < fileSizeInBlocks; i++){
      int off = libc.lseek(fd,  i * BLOCK_SIZE, 0 /*SEEK_SET*/);      
      long len = libc.read(fd, p, BLOCK_SIZE);
    }
    LOG.info("Done in "+ (System.currentTimeMillis() -start)+"ms");
    
    LOG.info("Disable caching:");
    
    CLib.trySkipCache(fd, 0, fileSizeInBlocks * BLOCK_SIZE);
    start = System.currentTimeMillis();
    for( int i=0; i < fileSizeInBlocks; i++){
      int off = libc.lseek(fd,  i * BLOCK_SIZE, 0 /*SEEK_SET*/);      
      long len = libc.read(fd, p, BLOCK_SIZE);
    }
    LOG.info("Done in "+ (System.currentTimeMillis() -start)+"ms");
    fis.close();
  }

  private static void testDirectRead() throws IOException {
    
    LOG.info("Reading file O_DIRECT, using native 'read' API");
    ByteBuffer buf = ByteBuffer.allocateDirect(2 * BLOCK_SIZE);
    long address = Utils.getBufferAddress(buf);
    long aoff = Utils.getAlignedOffset(buf, BLOCK_SIZE);
    address += aoff; 
    Pointer p = new Pointer(address);
    
    LOG.info("Aligned offset="+aoff);
    
    LibC libc = CLib.LIBC;
    FileInputStream fis =  new FileInputStream(file);
    fd = CLib.getfd(fis.getFD());
    
    boolean result = CLib.setRawIO(fd);
    
    CLib.disableReadAhead(fd, 0, fileSizeInBlocks * BLOCK_SIZE);
    
    LOG.info("Raw IO result="+result);
    if(result == false){
      return;
    }
    
    long start = System.currentTimeMillis();
    for( int i=0; i < fileSizeInBlocks; i++){
      int off = libc.lseek(fd,  i * BLOCK_SIZE, 0 /*SEEK_SET*/);      
      long len = libc.read(fd, p, BLOCK_SIZE);
    }
    LOG.info("Done in "+ (System.currentTimeMillis() -start)+"ms");
    
    LOG.info("Disable caching:");
    
    CLib.trySkipCache(fd, 0, fileSizeInBlocks * BLOCK_SIZE);
    start = System.currentTimeMillis();
    for( int i=0; i < fileSizeInBlocks; i++){
      int off = libc.lseek(fd,  i * BLOCK_SIZE, 0 /*SEEK_SET*/);      
      long len = libc.read(fd, p, BLOCK_SIZE);
    }
    
    LOG.info("Done in "+ (System.currentTimeMillis() -start)+"ms");
    
    start = System.currentTimeMillis();
    for( int i=0; i < fileSizeInBlocks; i++){
      int off = libc.lseek(fd,  i * BLOCK_SIZE, 0 /*SEEK_SET*/);      
      long len = libc.read(fd, p, BLOCK_SIZE);
    }
    
    LOG.info("Done in "+ (System.currentTimeMillis() -start)+"ms");
    fis.close();
  }
  
  private static void writeData() throws IOException {
    FileOutputStream fos = new FileOutputStream(file);
    

    LOG.info("fd="+fd);
    for(int i=0; i < fileSizeInBlocks; i++ ){
      fos.write(data);
    }
    fos.close();
    fos.flush();
  }
}
