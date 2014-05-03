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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndexer;
import org.apache.hadoop.io.RawComparator;
import org.junit.experimental.categories.Category;
import static com.koda.integ.hbase.test.TestUtils.appendToByteBuffer;



@Category(SmallTests.class)
public class TestHFileBlockIndexerMVCC extends TestCase{

  final static Log LOG = LogFactory.getLog(TestHFileBlockIndexerMVCC.class);
  
  static KeyValue[] array ;
  static ByteBuffer buffer;
  static int N = 1300;
  static int bufferSize = 100* 1024;
  static byte[] CF = new byte[]{'f'};
  static byte[] CQ = new byte[]{'c'};
  static int[] indexData;
  static long memstoreTS = 1234567;
  static KVComparator comparator = KeyValue.COMPARATOR;

  
  @Override
  protected void setUp()
  {
    if(buffer != null) return;
    buffer = ByteBuffer.allocate(bufferSize);
    array = new KeyValue[N];
    populateData();
    serializeData();
    try {
      indexBlock();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  private void serializeData() {
    
    for(int i =0; i < N; i++){
        writeKV(buffer, array[i]);
    }
    buffer.flip();
    LOG.info("Serialized buffer length="+buffer.limit());
  }

  private void writeKV(ByteBuffer buf, KeyValue kv) {    
    appendToByteBuffer(buf, kv, true);        
  }

  
  public void indexBlock() throws IOException
  {
      LOG.info("Block indexer starts");
      
      indexData = HFileBlockIndexer.createIndex(buffer, true, false);
      
      assertEquals(N, indexData.length);
      
     // dumpIndex(indexData);
      
     for(int i=0; i < N; i++){
        KeyValue kv = getKeyValue(indexData[i]);
        //LOG.info(kv);
        assertEquals(array[i], kv);
     }
           
     LOG.info("Block indexer finished");
  }
  
//  @Test 
//  public void testSeekExactFromArray()
//  {
//    LOG.info("Test seek exact from array starts");
//    int limit = buffer.limit();
//
//    for(int i = 0; i < N; i++){
//      buffer.position(0);
//      buffer.limit(limit);
//      KeyValue kv = array[i];
//      //LOG.info("Looking for: "+kv);
//      int offset = HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), indexData);
//      assertTrue(offset >=0);
//      KeyValue kvFound = getKeyValue(offset);
//      //LOG.info("Found      : "+kvFound);
//      assertEquals(kv, kvFound);        
//    }   
//    LOG.info("Test seek exact from array finished.");
//
//  }
  
  
//  @Test
//  public void testNotFoundLargereKV(){
//    
//    LOG.info("Test not found larger KV started.");
//    int limit = buffer.limit();
//    KeyValue largest = array[array.length-1];
//    byte[] row = largest.getRow();
//    String ss = new String(row);
//    for(int i=0; i < 1000; i ++){
//      buffer.position(0);
//      buffer.limit(limit);
//      // Create kv which is large than the largest
//      KeyValue kv = new KeyValue((ss+i).getBytes(), CF, CQ, Integer.toString(i).getBytes());
//      int offset = HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), indexData);
//      assertTrue(offset < 0);
//    }
//    buffer.position(0);
//    buffer.limit(limit);
//    
//    LOG.info("Test not found larger KV finished.");
//    
//  }
  
//  @Test
//  public void testNotFoundForReseek(){
//    
//    LOG.info("Test not found for reseek started.");
//    int limit = buffer.limit();
//
//    for(int i= 100; i < 200; i ++){
//      // set position larger than KV in a buffer
//      buffer.position(indexData[i+1]);
//      buffer.limit(limit);
//      // Create kv which is large than the largest
//      KeyValue kv = array[i];
//      int offset = HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), indexData);
//      assertTrue(indexData[i+1] == offset );
//    }
//    buffer.position(0);
//    buffer.limit(limit);
//    
//    LOG.info("Test not found for reseek finished.");
//    
//  }
  
//  @Test
//  public void testReseekStarted(){
//    
//    LOG.info("Test reseek started (relative).");
//    int limit = buffer.limit();
//
//    for(int i= 100; i < 200; i ++){
//      // set position smaller than KV in a buffer
//      buffer.position(indexData[i-1]);
//      buffer.limit(limit);
//      // Create kv which is large than the largest
//      KeyValue kv = array[i];
//      int offset = HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), indexData);
//      assertTrue(offset >= 0);
//      assertEquals(indexData[i], offset);
//    }
//    buffer.position(0);
//    buffer.limit(limit);
//    
//    LOG.info("Test reseek finished (relative).");
//    
//  }
  
//  @Test
//  public void testCornerCase()
//  {
//    LOG.info("Test corner case (seek KV which is smaller than the smallest) started.");
//    int limit = buffer.limit();
//    KeyValue kv = array[0]; 
//    
//    int offset = HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength() -1, indexData);
//    assertTrue(offset == 0);
//    buffer.position(0);
//    buffer.limit(limit);
//    LOG.info("Test corner case (seek KV which is smaller than the smallest) finished.");
//  }
//  
//  @Test
//  public void testSeekExactAbsolute()
//  {
//    LOG.info("Test seek exact absolute starts");
//    int limit = buffer.limit();
//    Random r = new Random();
//    for(int i=0; i < 100; i++){
//      buffer.position(0);
//      buffer.limit(limit);
//      int index = r.nextInt(N);
//      KeyValue kv = array[index];
//      //LOG.info("Looking for: "+kv);
//      int offset = HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), indexData);
//      assertTrue(offset >=0);
//      KeyValue kvFound = getKeyValue(offset);
//      //LOG.info("Found      : "+kvFound);
//      assertEquals(kv, kvFound);        
//    }
//    
//    LOG.info("Test seek exact absolute finished");
//  }
//  
//  @Test
//  public void testSeekNonExactAbsolute()
//  {
//    LOG.info("Test seek exact absolute negative starts");
//    int limit = buffer.limit();
//    Random r = new Random();
//    for(int i=0; i < 100; i++){
//      buffer.position(0);
//      buffer.limit(limit);
//      int index = N + r.nextInt(N);
//      byte[] row = getRow(index);
//      KeyValue kv = new KeyValue(row, CF, CQ, Integer.toString(index).getBytes());
//      //LOG.info("Looking for: "+kv);
//      int offset = HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), indexData);
//      if(offset < 0){
//        // Verify that the largest KV in array is less than 'kv'
//        assertTrue(KeyValue.COMPARATOR.compare(array[N-1], kv) < 0);
//        LOG.info("Not found.");
//      }
//      // else
//      // we found offset of a KV which is greater or equals than 'kv'
//      // find index
//      int k = Arrays.binarySearch(indexData, offset);
//      assertTrue( k >=0 );
//      assertTrue(KeyValue.COMPARATOR.compare(array[k], kv) >= 0);
//      //LOG.info("Found      : "+array[k]);
//
//      if( k > 0){
//        assertTrue(KeyValue.COMPARATOR.compare(array[k-1], kv) < 0);
//      }
//            
//    }
//    
//    LOG.info("Test seek non-exact absolute  finished");
//  }
//  
//  @Test
//  public void testSeekPerformance()
//  {
//    LOG.info("Test seek performance started.");
//    int total = 10000;
//    int num   = 1000;
//    long result = 0;
//    long start = System.currentTimeMillis();
//    for(int i=0; i < num; i++)
//    {
//      result += seek(total);
//    }
//    
//    long end = System.currentTimeMillis();
//    LOG.info("Test seek performance finished. result="+result+" in "+(end-start)+"ms. "+((long)(total * num) * 1000/ (end -start))+" seek per sec");
//
//  }
//  Random rand = new Random();
//  
//  private final long seek(int n)
//  {
//    long ret = 0;
//    int limit = buffer.limit();
//    for(int i=0; i < n; i++){
//      buffer.position(0);
//      buffer.limit(limit);
//      int index = rand.nextInt(N);
//      KeyValue kv = array[index];   
//      ret += HFileBlockIndexer.seekAfter(buffer, kv.getBuffer(), kv.getRowOffset(), kv.getRowLength(), indexData);
//    }
//    return ret;
//  }
  
  private KeyValue getKeyValue(int off) {
    byte[] buf = buffer.array();
    off += buffer.arrayOffset();
    
    KeyValue kv = new KeyValue(buf, off);
    return kv;
  }

  @SuppressWarnings("unused")
  private void dumpIndex(int[] data) {
    // TODO Auto-generated method stub
    LOG.info("Block index:");
    for(int i=0; i < data.length; i++)
    {
      LOG.info("i="+i+" off="+data[i]);
    }
  }

  private void populateData() {

      LOG.info("Populating data ... ");
      for(int i=0; i < N; i++){
        byte[] row = getRow(i);
        array[i] = new KeyValue(row, CF, CQ, Integer.toString(i).getBytes());
        array[i].setMemstoreTS(memstoreTS);
      }
      // Now we need to sort array
      Arrays.sort(array, KeyValue.COMPARATOR);
      LOG.info("Done");
  }


    // 1 year = 13; 2 months = 09/10: 30 days: 5 actions: [1..5]
    // 100 interactions per day -> 6000 avg total
    // users = 200 for start
    
    Random r = new Random();
    
    private String getYear(){
      return "2013";
    }
    
    private  String getMonth(){
      switch (r.nextInt(2)){
        case 0: return "09";
        case 1: return "10";
      }
      return null;
    }
    
    private String getAction()
    {
        int n = r.nextInt(5);
        switch(n){
          case 0: return "01";
          case 1: return "02";
          case 2: return "03";
          case 3: return "04";
          case 4: return "05";
        }
        return null;
    }
    
    private String getUserId(){
      int n = r.nextInt(200)+1;
      return format(n, 4);
    }
    
    private String getDay()
    {
      int n = r.nextInt(30)+1;
      return format(n,2);
    }
    
    
    private String format(int n, int pos) {
      String s = Integer.toString(n);
      if(s.length() > pos) return s.substring(s.length() - pos);
      for(int i= s.length(); i < pos; i++)
      {
          s = "0" + s;
      }
      return s;
    }

    private byte[] getRow(int i) {
      // Format of row:
      // userId_actionId_year_month_day_someid
      // For testing purposes:
      // userId = 4 bytes (create 1000 users)
      // actionId = 2 byte
      // year = 4 bytes
      // month = 2 bytes
      // day   = 2 bytes
      // someid = 4 bytes
      StringBuffer sb = new StringBuffer();
      sb.append(getUserId()).append(getAction()).append(getYear()).
          append(getMonth()).append(getDay()).append(format(i,7));
      return sb.toString().getBytes();
    }

  
}
