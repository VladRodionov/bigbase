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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileCutter {

  static int counter =0;
  
  static String header = 
    
    "/*******************************************************************************\n" +
     "* Copyright (c) 2013 Vladimir Rodionov. All Rights Reserved\n" +
     "*\n"+ 
     "* This code is released under the GNU Affero General Public License.\n"+ 
     "*\n"+
     "* See: http://www.fsf.org/licensing/licenses/agpl-3.0.html\n"+
     "*\n"+
     "* VLADIMIR RODIONOV MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY\n"+
     "* OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE\n"+
     "* IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR\n"+
     "* NON-INFRINGEMENT. Vladimir Rodionov SHALL NOT BE LIABLE FOR ANY DAMAGES SUFFERED\n"+
     "* BY LICENSEE AS A RESULT OF USING, MODIFYING OR DISTRIBUTING THIS SOFTWARE OR\n"+
     "* ITS DERIVATIVES.\n"+
     "*\n"+ 
     "* Author: Vladimir Rodionov\n"+
     "*\n"+
     "*******************************************************************************/\n";   
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    // Expects root directory
    
    String directory = args[0];
    processFiles(new File(directory));
    
    System.out.println("Processed "+ counter +" files");
  }

  private static void processFiles(File f) throws IOException {
    if(f.isFile()){
      processFile(f); 
      return;
    }
    
    File[] list = f.listFiles();
    
    for(File file: list){
      if(file.isFile()){
        processFile(file);
      } else {
        processFiles(file);
      }
      
    }   
    //System.out.println("Processed "+counter+" java source files");
  }

  private static void processFile(File file) throws IOException {

    
    if( file.getAbsolutePath().endsWith(".java") == false){
      System.out.println("Skipping "+ file.getAbsolutePath());
      return;
    } 
    
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    byte[] buffer = new byte[(int)file.length()];
    dis.readFully(buffer);
    String content = new String(buffer);
    
    int offset = content.indexOf("package");
    
    if(offset < 10){
      System.out.println(file.getAbsolutePath()+" does not have header. ");
      
    }
    counter++;
    content = content.substring(offset);
    dis.close();
    
    FileOutputStream out = new FileOutputStream (file);
    out.write(header.getBytes());
    out.write(content.getBytes());
    out.close();
    System.out.println("updated " + file.getAbsolutePath());

  }
}
