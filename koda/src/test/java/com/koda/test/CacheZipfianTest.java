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
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Random;
//
//import junit.framework.TestCase;
//
//import org.apache.log4j.Logger;
//
//import com.koda.KodaException;
//import com.koda.NativeMemoryException;
//import com.koda.cache.CacheManager;
//import com.koda.cache.OffHeapCache;
//import com.koda.config.CacheConfiguration;
//import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
//import com.yahoo.ycsb.generator.IntegerGenerator;
//import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
//
//public class CacheZipfianTest extends TestCase{
//  
//  private final static Logger LOG = Logger
//  .getLogger(CacheZipfianTest.class);
//  
//  static OffHeapCache sCache;
//  static int N = 1000000;
//  /** The SIZE. */
//  //private static int SIZE = 100;
//  static long memoryLimit = 150000000;
//  static IntegerGenerator generator ;
//  static long M = 10000000;
//  static long total = 0;
//  static long hits = 0;
//  static{
//    try {
//      initCache();
//    } catch (KodaException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//    }
//  }
//
//  private static void initCache() throws KodaException {
//    CacheManager manager = CacheManager.getInstance();
//    CacheConfiguration config = new CacheConfiguration();
//    config.setBucketNumber(N);
//    config.setMaxMemory(memoryLimit);
//    config.setEvictionPolicy("LRU");
//   //config.setLRU2QInsertPoint(0.3);
//    config.setCacheName("test");
//    config.setCandidateListSize(30);
//    sCache = manager.createCache(config);
//    //generator = new ScrambledZipfianGenerator(0, M-1 );
//    generator = new HotspotIntegerGenerator(0, (int) (M-1), 0.1, 0.9);
//    
//  }
//  
//  public void _testLoadCache() throws NativeMemoryException, IOException
//  {
//    LOG.info("Loading cache until its full ");
//    String key  ="key-";
//    String value = "value-";
//    //int total = 0;
//    long startTime = System.currentTimeMillis();
//    while( total < M * 10){
//      int idx = generator.nextInt();
//      String skey = key + idx;
//      if(sCache.contains(skey)){
//        hits++;
//      }  else{
//        sCache.put(skey, value+total);
//      }
//      total++;
//      if(total > 0 && total % 100000 == 0){
//        LOG.info(total+" : Items: "+sCache.size() +" hits: "+hits+" hit-ratio="+(double) hits*100/total+"%%");
//      }
//    }
//    long endTime = System.currentTimeMillis();
//    LOG.info("Done in "+(endTime - startTime)+"ms. Total objects="+sCache.size());
//
//    LOG.info("Done");
//  }
//  
//  Random r = new Random();
//  
//  public void _testLoadCache2() throws NativeMemoryException, IOException
//  {
//    LOG.info("Loading cache until its full ");
//    String key  ="key-";
//    String value = "value-";
//    //int total = 0;
//    long startTime = System.currentTimeMillis();
//    int miss = 0;
//    int hits = 0;
//    int total = 0;
//    int count = 0;
//    while( count++ < M * 10){
//      int n = r.nextInt(100);
//      if( n < 50) { //PUT
//        String skey = key + total;
//        sCache.put(skey, skey);
//        //if(sCache.contains(skey) == false){
//        //  if(sCache.isEvictionActive() == false){
//        //    LOG.info("MISSED: "+skey);
//        //  }
//        //}
//        total++;
//      } else{ // GET
//          int size = (int)sCache.size();
//          if(size == 0) continue;
//          int x = r.nextInt(size);
//          String skey = key + (total - x - 1); 
//          if(sCache.contains(skey)){
//            hits++;
//          } else{
//            if(sCache.isEvictionActive() == false){
//              LOG.info("MISSED: "+skey+" SIZE="+sCache.size()+" TOTAL="+total);
//            }
//            miss++;
//          }
//          
//      }
//      if( count > 0 && count % 100000 == 0){
//        LOG.info(total+" : Items: "+sCache.size() +" hits: "+hits+" miss: "+miss + 
//            " hit-ratio="+(double) hits*100/(hits+miss)+"%%");
//      }
//    }
//    long endTime = System.currentTimeMillis();
//    LOG.info("Done in "+(endTime - startTime)+"ms. Total objects="+sCache.size());
//
//    LOG.info("Done");
//  }
//
//  int numRecords = 20000;
//  int cacheSize = 7750;
//  int youngSize =(int) (0.25 * cacheSize);
//  int tenSize =  (int)(0.75 * cacheSize);
//  
//  //CachedItem[] items = new CachedItem[numRecords];
//  List<CachedItem> itemCache = new ArrayList<CachedItem>();
//  List<CachedItem> itemCacheTenured = new ArrayList<CachedItem>();  
//  List<CachedItem>  itemCacheYoung = new ArrayList<CachedItem>();
//  
//  long sequence = 0;
//  
//  public void testRealLRU()
//  {
//    LOG.info("Test real LRU");
//    //generator = new HotspotIntegerGenerator(0, (int) (numRecords-1), 0.2, 0.6);
//    //LOG.info("Start calculation:");
//    //long start = System.currentTimeMillis();
//    generator = new ScrambledZipfianGenerator(0, numRecords-1);
//    //LOG.info("Done in "+(System.currentTimeMillis() - start)+"ms");
//    int hits = 0;
//    int miss = 0;    
//
//    int count = 0;
//    while( count++ < 20 * numRecords){
//      int id = generator.nextInt();
//      if(contains(id, itemCache) != null){
//        hits++;
//      } else{
//        miss ++;
//        CachedItem evicted = evict(itemCache, cacheSize);
//        if(evicted != null){
//          //LOG.info("Evicted "+evicted.seqId);
//        }
//        sequence++;
//        CachedItem item = new CachedItem();
//        item.id = id;
//        item.seqId = sequence;
//        itemCache.add(item);
//      }
//    }
//    
//    LOG.info("Hit ratio after "+count+" iterations = "+(double) hits/(hits+miss));
//  }
//  
//  public void testRealLRU2Q()
//  {
//    LOG.info("Test real LRU-2Q");
//    //generator = new HotspotIntegerGenerator(0, (int) (numRecords-1), 0.2, 0.6);
//    //LOG.info("Start calculation:");
//    //long start = System.currentTimeMillis();
//    generator = new ScrambledZipfianGenerator(0, numRecords-1);
//    //LOG.info("Done in "+(System.currentTimeMillis() - start)+"ms");
//    int hits = 0;
//    int miss = 0;    
//    sequence = 0;
//    int count = 0;
//    while( count++ < 20 * numRecords){
//      int id = generator.nextInt();
//      CachedItem item = null;
//      if((item = contains(id, itemCacheYoung)) != null){
//        hits++;
//        //CachedItem item = evict(itemCacheYoung, youngSize);
//        remove(itemCacheYoung, id);
//        item.seqId = ++sequence;
//        evict(itemCacheTenured, tenSize);
//        itemCacheTenured.add(item);
//      } else if( contains(id, itemCacheTenured) != null){
//        hits++;
//      } else{
//        miss ++;
//        CachedItem evicted = evict(itemCacheYoung, youngSize);
//
//        sequence++;
//        item = new CachedItem();
//        item.id = id;
//        item.seqId = sequence;
//        itemCacheYoung.add(item);
//      }
//    }
//    
//    LOG.info("Hit ratio after "+count+" iterations = "+(double) hits/(hits+miss));
//  }
//  
//  public void testRealLRU2Qe()
//  {
//    LOG.info("Test real LRU-2Qe");
//    //generator = new HotspotIntegerGenerator(0, (int) (numRecords-1), 0.2, 0.6);
//    //LOG.info("Start calculation:");
//    //long start = System.currentTimeMillis();
//    generator = new ScrambledZipfianGenerator(0, numRecords-1);
//    //LOG.info("Done in "+(System.currentTimeMillis() - start)+"ms");
//    int hits = 0;
//    int miss = 0;    
//    sequence = 0;
//    int count = 0;
//    while( count++ < 20 * numRecords){
//      int id = generator.nextInt();
//      CachedItem item = null;
//      if((item = contains(id, itemCacheYoung)) != null){
//        hits++;
//        //CachedItem item = evict(itemCacheYoung, youngSize);
//        remove(itemCacheYoung, id);
//        item.seqId = ++sequence;
//        CachedItem evicted = evict(itemCacheTenured, tenSize);
//        itemCacheTenured.add(item);
//        if(evicted != null) {
//          evicted.seqId = ++sequence;
//          itemCacheYoung.add(evicted);
//        }
//      } else if( contains(id, itemCacheTenured) != null){
//        hits++;
//      } else{
//        miss ++;
//        CachedItem evicted = evict(itemCacheYoung, youngSize);
//
//        sequence++;
//        item = new CachedItem();
//        item.id = id;
//        item.seqId = sequence;
//        itemCacheYoung.add(item);
//      }
//    }
//    
//    LOG.info("Hit ratio after "+count+" iterations = "+(double) hits/(hits+miss));
//  }
//  
//  private void remove(List<CachedItem> cache, int id) {
//    for(int i=0; i < cache.size(); i++){
//      if( id == cache.get(i).id) {
//        // Update 'time'
//        cache.remove(i);
//      }
//    }
//    
//  }
//
//  private CachedItem contains(int id, List<CachedItem> cache)
//  {
//     for(int i=0; i < cache.size(); i++){
//       if( id == cache.get(i).id) {
//         // Update 'time'
//         cache.get(i).seqId = (int)(++sequence);
//         return cache.get(i);
//       }
//     }
//     return null;
//  } 
//
//  private CachedItem evict(List<CachedItem> cache, int size)
//  {
//    if(cache.size() < size) return null;
//    Collections.sort(cache);    
//    return cache.remove(0);
//  
//  }
//  
//  
//}
//
//
//
//class CachedItem implements Comparable<CachedItem>{
//  Integer id;
//  long    seqId;
//  @Override
//  public int compareTo(CachedItem other) {
//    return (int) (seqId - other.seqId);
//  }
//         
//}
//

