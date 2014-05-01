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
package com.koda.test;

import java.nio.ByteBuffer;
import java.util.Random;

import com.koda.compression.lz4.LZ4;

public class CompactionRatioTest {

  
  
  public static void main(String[] args){
    // Sizes from 100
    ByteBuffer src = ByteBuffer.allocateDirect(2000);
    ByteBuffer dst = ByteBuffer.allocateDirect(2000);
    String key = "user186457";
    String field = "field";
    int fieldNumber = 10;
    int fieldLength = 100;
    Random r = new Random();
    for (int i = 1 ; i < fieldLength; i++){
      byte[] baza = key.getBytes();
      for(int k = 0; k < fieldNumber; k++){
        baza = append(baza, (field + k).getBytes());
        byte[] base = new byte[i];      
        r.nextBytes(base);
        base = append(base, (byte)0, fieldLength -i);
        baza = append(baza, base);
        //System.out.println(baza.length);
        

      }
      src.clear();
      src.put(baza);
      src.flip();
      dst.clear();
      int size = LZ4.compress(src, dst);
      System.out.println(i +" src="+ baza.length+" compressed="+size+" ratio="+ (double) baza.length/size);      
    }
  }
  
  private static byte[] append(byte[] src, byte[] other)
  {
    byte[] result = new byte[src.length + other.length];
    System.arraycopy(src, 0, result, 0, src.length);
    System.arraycopy(other, 0, result, src.length, other.length);
    return result;
  }
  
  
  private static byte[] append(byte[] src, byte what, int howMany)
  {
    byte[] result = new byte[src.length + howMany];
    System.arraycopy(src, 0, result, 0, src.length);
    for(int i=0; i < howMany; i++){
       result[src.length + i] = what;
    }
    
    return result;
  }
}
