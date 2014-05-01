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
package com.koda.common.test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;


import com.koda.common.lcm.LcmContainer;
import com.koda.common.lcm.Tool;

import junit.framework.TestCase;

public class ToolTest extends TestCase{
  //private final static Logger LOG = Logger.getLogger(ToolTest.class);
  
  public void testBasic() throws IOException
  {
    System.out.println("Test basic started");
    TestObject obj = new TestObject("key", "value");
    String s = Tool.doObject(obj);  
    
    System.out.println("Serialized:\n"+s);
    TestObject des = (TestObject) Tool.undoObject(s);
    assertEquals(obj.toString(), des.toString());
    
    System.out.println("Test basic finished");
  }
  
  public void testBasicFile() throws IOException
  {
    System.out.println("Test basic file started");
    TestObject obj = new TestObject("key", "value");
    String s = Tool.doObject(obj);  
    
    System.out.println("Serialized:\n"+s);
    File f = File.createTempFile("pre", "sfx");
    FileOutputStream fos = new FileOutputStream(f);
    DataOutputStream dos = new DataOutputStream(fos);
    dos.writeUTF(s);
    dos.close();
    
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String ss = dis.readUTF();
    dis.close();
    f.deleteOnExit();
    TestObject des = (TestObject) Tool.undoObject(ss);
    assertEquals(obj.toString(), des.toString());
    
    System.out.println("Test basic file finished");
  }
  
  
  public void testLcmContainerFile() throws IOException
  {
    System.out.println("Test Lcm file started");
    
    LcmContainer cont = new LcmContainer();
    cont.setProperty("name1", "value1");
    cont.setProperty("name2", "value2");
    cont.setProperty("name3", "value3");
    cont.setProperty("name4", "value4");
    cont.setProperty("name5", "value5");
    
    String s = Tool.doObject(cont);  
    
    System.out.println("Serialized:\n"+s);
    File f = File.createTempFile("pre", "sfx");
    FileOutputStream fos = new FileOutputStream(f);
    DataOutputStream dos = new DataOutputStream(fos);
    dos.writeUTF(s);
    dos.close();
    
    FileInputStream fis = new FileInputStream(f);
    DataInputStream dis = new DataInputStream(fis);
    String ss = dis.readUTF();
    dis.close();
    f.deleteOnExit();
    LcmContainer des = (LcmContainer) Tool.undoObject(ss);
    assertTrue(cont.equals(des));
    
    System.out.println("Test Lcm file finished");
  }
}


class TestObject implements Serializable
{

  private static final long serialVersionUID = -8580248265577524983L;
  
  private String key;
  private String value;
  
  public TestObject(String s1, String s2 )
  {
    this.key = s1;
    this.value = s2;
    
  }
  
  public String toString()
  {
    return key+":"+value;
  }
}