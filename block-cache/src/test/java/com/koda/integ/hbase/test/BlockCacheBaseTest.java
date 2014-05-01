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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.koda.integ.hbase.blockcache.OffHeapBlockCache;


// TODO: Auto-generated Javadoc
/**
 * The Class CoprocessorLoadTest.
 */
public class BlockCacheBaseTest extends BaseTest{

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(BlockCacheBaseTest.class);
    
  /** The util. */
  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();  
    
  /** The n. */
  int N = 100000;
  
  /** The cache size. */
  private static long cacheSize = 2000000000L; // 2G
  
  /** The cache impl class. */
  private static String cacheImplClass =OffHeapBlockCache.class.getName();//;
  
  /** The young gen factor. */
  @SuppressWarnings("unused")
  private static Float youngGenFactor = 0.5f;
  
  /** The cache compression. */
  private static String cacheCompression = "LZ4";
  
  /** The cache overflow enabled. */
  private static boolean cacheOverflowEnabled = false;
  
  /** The on heap cache ratio. */
  private static float   onHeapCacheRatio = 0.2f;

  /** The bloom block size. */
  private static int BLOOM_BLOCK_SIZE = 64 * 1024;
  
  /** The index block size. */
  private static int INDEX_BLOCK_SIZE = 64 * 1024; 
    
  /** The cluster. */
  MiniHBaseCluster cluster;
    
  /** The _table c. */
  HTable _tableA ;
  
  private static boolean initDone = false;
  
  /* (non-Javadoc)
   * @see junit.framework.TestCase#setUp()
   */
  @Override
  public void setUp() throws Exception {
    
    if(initDone) return;
    
    ConsoleAppender console = new ConsoleAppender(); // create appender
    // configure the appender
    String PATTERN = "%d [%p|%c|%C{1}] %m%n";
    console.setLayout(new PatternLayout(PATTERN));
    console.setThreshold(Level.WARN);

    console.activateOptions();
    // add appender to any Logger (here is root)
    Logger.getRootLogger().removeAllAppenders();
    Logger.getRootLogger().addAppender(console);
    Configuration conf = UTIL.getConfiguration();
    conf.set("hbase.zookeeper.useMulti", "false");

    // Cache configuration
    conf.set(OffHeapBlockCache.BLOCK_CACHE_MEMORY_SIZE, Long.toString(cacheSize));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_IMPL, cacheImplClass);
    //conf.set(OffHeapBlockCache.BLOCK_CACHE_YOUNG_GEN_FACTOR, Float.toString(youngGenFactor));
    conf.set(OffHeapBlockCache.BLOCK_CACHE_COMPRESSION, cacheCompression);
    conf.set(OffHeapBlockCache.BLOCK_CACHE_OVERFLOW_TO_EXT_STORAGE_ENABLED, Boolean.toString(cacheOverflowEnabled));
    conf.set("io.storefile.bloom.block.size", Integer.toString(BLOOM_BLOCK_SIZE));
    conf.set("hfile.index.block.max.size", Integer.toString(INDEX_BLOCK_SIZE));
    conf.set(OffHeapBlockCache.HEAP_BLOCK_CACHE_MEMORY_RATIO, Float.toString(onHeapCacheRatio));    

    // Enable snapshot
    UTIL.startMiniCluster(1);
    initDone = true;
    if( data != null) return;   
    data = generateData(N);  
    cluster = UTIL.getMiniHBaseCluster();
    createTables(VERSIONS);
    createHBaseTables();
    putAllData(_tableA, N);      
      
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
    if( admin.tableExists(tableA.getName()) == true){
      LOG.info("Deleting table "+tableA);
      admin.disableTable(tableA.getName());
      admin.deleteTable(tableA.getName());
      LOG.info("Deleted table "+tableA);
    }
    admin.createTable(tableA);
    LOG.info("Created table "+tableA);
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
    table.setAutoFlush(false);
    long start = System.currentTimeMillis();
    for(int i=0; i < n; i++){
      Put put = createPut(data.get(i));

      if( i % 10000 == 0){
        System.out.println("put: "+i);
      }
      table.put(put);
    }
    table.flushCommits();
    LOG.error ("Put all " +n +" rows  finished in "+(System.currentTimeMillis() - start)+"ms");
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
}
