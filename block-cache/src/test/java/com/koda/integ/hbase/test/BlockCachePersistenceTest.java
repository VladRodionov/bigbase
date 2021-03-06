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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.koda.integ.hbase.blockcache.OffHeapBlockCache;


// TODO: Auto-generated Javadoc
/**
 * The Class CoprocessorLoadTest.
 */
public class BlockCachePersistenceTest extends BaseTest{

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(BlockCachePersistenceTest.class);
    
  /** The util. */
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();  
  
  
  /** The n. */
  static int N = 10000;
  
  
  /** The cluster. */
  static MiniHBaseCluster cluster;
  
  
  /** The _table c. */
  static HTable _tableA;
  
  static boolean loadOnStartup = false;
  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  @Override
  public void setUp() throws Exception {
//    ConsoleAppender console = new ConsoleAppender(); // create appender
//    // configure the appender
//    String PATTERN = "%d [%p|%c|%C{1}] %m%n";
//    console.setLayout(new PatternLayout(PATTERN));
//    console.setThreshold(Level.ERROR);
//
//    console.activateOptions();
//    // add appender to any Logger (here is root)
//    Logger.getRootLogger().removeAllAppenders();
//    Logger.getRootLogger().addAppender(console);
    Configuration conf = UTIL.getConfiguration();

    conf.set("hbase.zookeeper.useMulti", "false");

    // Cache configuration
    conf.set(OffHeapBlockCache.BLOCK_CACHE_MEMORY_SIZE, "1000000000");
    //conf.set(CacheConfiguration.EVICTION_POLICY, "LRU");

    conf.set(OffHeapBlockCache.BLOCK_CACHE_COMPRESSION, "LZ4");
    
    // set persistent cache
    conf.set(OffHeapBlockCache.BLOCK_CACHE_PERSISTENT, Boolean.toString(true));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_DATA_ROOTS, "/tmp/ramdisk/data");
    File file = new File("/tmp/ramdisk/data/block-cache.dat");
    if(file.exists()){
      loadOnStartup = true;
    }
    
    // Enable snapshot
    UTIL.startMiniCluster(1);
    
    // Row Cache
    if( data != null) return;   
    data = generateData(N);  
    cluster = UTIL.getMiniHBaseCluster();
    createTables(VERSIONS);
    createHBaseTables();
      
      
  }
  
  

  /* (non-Javadoc)
   * @see com.inclouds.hbase.test.BaseTest#createTables()
   */

  /**
   * Creates the h base tables.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void createHBaseTables() throws IOException {   
    Configuration cfg = cluster.getConf();
    HBaseAdmin admin = new HBaseAdmin(cfg);
    if( admin.tableExists(tableA.getName()) == false){
      admin.createTable(tableA);
      LOG.error("Created table "+tableA);
    }
     
    _tableA = new HTable(cfg, TABLE_A);   
    
  }



  /* (non-Javadoc)
   * @see junit.framework.TestCase#tearDown()
   */
  @Override 
  public void tearDown() throws Exception {
      LOG.error("\n Tear Down the cluster and test \n");
    //Thread.sleep(2000);
      //UTIL.shutdownMiniCluster();
  }
    


  /**
   * Put all data.
   *
   * @param table the table
   * @param n the n
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void putAllData(HTable table, int n) throws IOException
  {
    LOG.error ("Put all " + n +" rows  starts.");
    long start = System.currentTimeMillis();
    for(int i=0; i < n; i++){
      Put put = createPut(data.get(i));
      table.put(put);
    }
    table.flushCommits();
    LOG.error ("Put all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
  }
  
  public void testFirstGet() throws IOException
  {
    
    LOG.error("Test first get started");
    
    long start = System.currentTimeMillis();
    for(int i=0 ; i< N; i++){   
      Get get = createGet(data.get(i).get(0).getRow(), null, null, null);
      
      //LOG.info(i+":"+ get);
//      if( i == 0) cache.setTrace(true);
//      else cache.setTrace(false);
      
      get.setMaxVersions(Integer.MAX_VALUE);
      Result result = _tableA.get(get);   
      //LOG.info(i+" Result is null = "+ result.isEmpty());
      List<KeyValue> list = result.list();
      assertEquals(data.get(i).size(), list.size()); 
      
      assertTrue(equalsNoTS(data.get(i), list));

//      if(loadOnStartup == false){
//        assertEquals(0, cache.getFromCache());
//      } 
      
    }
    LOG.error("Test first get finished in "+(System.currentTimeMillis() - start)+"ms");
    
  }
  
  
  
  /**
   * Test second get.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void testSecondGet() throws IOException
  {
    
    LOG.error("Test second get started");
    
    long start = System.currentTimeMillis();
    for(int i=0 ; i< N; i++){   
      Get get = createGet(data.get(i).get(0).getRow(), null, null, null);
      get.setMaxVersions(Integer.MAX_VALUE);
//      if( i == 0) cache.setTrace(true);
//      else cache.setTrace(false);
      //*DEBUG*/ LOG.info(i+":"+get);
      
      Result result = _tableA.get(get);   
      List<KeyValue> list = result.list();
      assertEquals(data.get(i).size(), list.size());  
      //assertEquals(list.size(), cache.getFromCache());
      assertTrue(equalsNoTS(data.get(i), list));
    }
    LOG.error("Test second get finished in "+(System.currentTimeMillis() - start)+"ms");
    
  }
  
  
  /**
   * Delete all data.
   *
   * @param table the table
   * @param n the n
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected void deleteAllData(HTable table, int n) throws IOException
  {
    LOG.error ("Delete all " + n +" rows  starts.");
    long start = System.currentTimeMillis();
    for(int i=0; i < n; i++){
      Delete delete = createDelete(data.get(i).get(0).getRow());
      delete.setTimestamp(start);
      table.delete(delete);
    }
    table.flushCommits();
    LOG.error ("Delete all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
  }

  /**
   * Filter.
   *
   * @param list the list
   * @param fam the fam
   * @param col the col
   * @return the list
   */
  protected List<KeyValue> filter (List<KeyValue> list, byte[] fam, byte[] col)
  {
    List<KeyValue> newList = new ArrayList<KeyValue>();
    for(KeyValue kv: list){
      if(doFilter(kv, fam, col)){
        continue;
      }
      newList.add(kv);
    }
    return newList;
  }
  
  /**
   * Do filter.
   *
   * @param kv the kv
   * @param fam the fam
   * @param col the col
   * @return true, if successful
   */
  private final boolean doFilter(KeyValue kv, byte[] fam, byte[] col){
    if (fam == null) return false;
    byte[] f = kv.getFamily();
    if(Bytes.equals(f, fam) == false) return true;
    if( col == null) return false;
    byte[] c = kv.getQualifier();
    if(Bytes.equals(c, col) == false) return true;
    return false;
  }
  
  /**
   * Dump put.
   *
   * @param put the put
   */
  protected void dumpPut(Put put) {
    Map<byte[], List<KeyValue>> map = put.getFamilyMap();
    for(byte[] row: map.keySet()){
      List<KeyValue> list = map.get(row);
      for(KeyValue kv : list){
        LOG.error(kv);
      }
    }
    
  }
  
  public static void main(String[] args) throws Exception
  {
    BlockCachePersistenceTest test = new BlockCachePersistenceTest();
    test.setUp();
    if(BlockCachePersistenceTest.loadOnStartup){
      //assertEquals(3 *N, cache.size());
      LOG.info("Loaded objects into cache on start up");
      //test.deleteAllData(CoprocessorCachePersistenceTest._tableA, CoprocessorCachePersistenceTest.N);
      //UTIL.compact(_tableA.getTableName(), true);
      //LOG.info("Cache size after deletion: "+cache.size());
      //assertEquals(0, cache.size());
    } else{
      LOG.info("No saved data found");
      test.putAllData(BlockCachePersistenceTest._tableA, BlockCachePersistenceTest.N);
    }   
    
    
    //test.deleteAllData(CoprocessorCachePersistenceTest._tableA, CoprocessorCachePersistenceTest.N);
    //test.putAllData(CoprocessorCachePersistenceTest._tableA, CoprocessorCachePersistenceTest.N);

    // done
    
    test.testFirstGet();

    test.testSecondGet();
    cluster.shutdown();
  }
}
