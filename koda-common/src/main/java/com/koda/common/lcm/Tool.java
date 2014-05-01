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
package com.koda.common.lcm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;


public class Tool {

    private static byte xorValue = (byte) 23;
    
    private static String doStuff(byte[] buffer)
    {
       // reverse bytes
       int lastIndex = buffer.length -1;
       for(int i=0; i < buffer.length/2; i++){
         byte tmp = buffer[i];
         buffer[i] = (byte)(buffer[lastIndex -i] ^ xorValue);
         buffer[lastIndex -i] = (byte)(tmp ^ xorValue);
       }
       
       return new String(Base64.encodeBase64(buffer));
    }
    
    
    private static byte[] undoStuff(String s)
    {
      byte[] buffer = null;
  
        buffer = Base64.decodeBase64(s.getBytes());
        // reverse bytes
        int lastIndex = buffer.length -1;
        for(int i=0; i < buffer.length/2; i++){
          byte tmp = buffer[i];
          buffer[i] = (byte)(buffer[lastIndex -i] ^ xorValue);
          buffer[lastIndex -i] = (byte)(tmp ^ xorValue);
        }
        
      
      return buffer;
    }
    
    public static String doObject(Serializable obj) throws IOException
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      return doStuff(baos.toByteArray());      
    }
    
    
    public static Object undoObject(String obj) throws IOException
    {
      ByteArrayInputStream is = new ByteArrayInputStream(undoStuff(obj));
      ObjectInputStream ois = new ObjectInputStream(is);
      try{
        return ois.readObject();
      } catch(ClassNotFoundException e){
        throw new IOException(e);
      }     
    }
    
    public static Object fromFile(String fileName) throws IOException
    {
      FileInputStream fis = new FileInputStream(fileName);
      DataInputStream dis = new DataInputStream(fis);
      String s = dis.readUTF();
      dis.close();
      return undoObject(s);     
    }
    
    public static void toFile(String fn, Serializable obj) throws IOException
    {
      FileOutputStream fos = new FileOutputStream(fn);
      DataOutputStream dos = new DataOutputStream(fos);
      dos.writeUTF(doObject(obj));
      
    }
    
    /**
     * Check if its 'superpath'
     * @param path
     * @return
     */
    public static boolean checkSPath(String path)
    {
        final byte[] buf = new byte[] {'-', '-', 'i','n', 'c', 'l','o', 'u', 'd', 's'};  
        if( path == null || path.length() != buf.length) return false;
        for(int i=0; i < buf.length; i++){
          if (buf[i] != path.charAt(i)) return false;
        }
        return true;
        
    }
}
