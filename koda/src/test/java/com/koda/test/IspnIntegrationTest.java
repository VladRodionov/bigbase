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
//import java.util.Collection;
//import java.util.Map;
//import java.util.Random;
//import java.util.Set;
//import java.util.concurrent.ExecutionException;
//
//import junit.framework.TestCase;
//
//import org.apache.log4j.Logger;
//import org.infinispan.Cache;
//import org.infinispan.config.Configuration;
//import org.infinispan.manager.DefaultCacheManager;
//import org.infinispan.manager.EmbeddedCacheManager;
//import org.infinispan.util.concurrent.NotifyingFuture;
//
//import com.koda.KodaException;
//import com.koda.NativeMemoryException;
//import com.koda.cache.OffHeapCache;
//import com.koda.integ.ispn.OffHeapDataContainer;
//
//// TODO: Auto-generated Javadoc
///**
// * The Class IspnIntegrationTest.
// */
//public class IspnIntegrationTest extends TestCase {
//
//	/** The Constant LOG. */
//	private final static Logger LOG = Logger.getLogger(IspnIntegrationTest.class);
//	
//	/** Infinispan cache instance. */
//	@SuppressWarnings("unchecked")
//	private static Cache ispnCacheKoda;
//	
//	/** OffHeapCache instance. */
//	
//	private static OffHeapCache mCache;
//	
//	/** The N. */
//	private static int N = 100000;// 2M 
//	
//	/** The s memory limit. */
//	private static long sMemoryLimit = 1000000000; // 1G
//	
//	/** The was set up. */
//	private static boolean wasSetUp = false;
//	
//	/* (non-Javadoc)
//	 * @see junit.framework.TestCase#setUp()
//	 */
//	@Override
//	protected void setUp() throws Exception {
//		if(wasSetUp) return;
//		initCacheInfinispanKoda();
//		fillCache();
//		wasSetUp = true;
//		super.setUp();
//	}
//	
//	/**
//	 * Fill cache.
//	 */
//	@SuppressWarnings("unchecked")
//	private void fillCache() {
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);
//		byte[] value = new byte[500];
//		LOG.info("Starting filling cache");
//		long t1 = System.currentTimeMillis();
//		for(int i=0; i < N; i++){
//			byte[] bkey = get(i, key);
//			byte[] bvalue = get(i, value);
//			ispnCacheKoda.put(bkey, bvalue);
//		}
//		long t2 = System.currentTimeMillis();
//		LOG.info("Loaded "+N+" key-values in " +(t2-t1)+" ms. Size="+ispnCacheKoda.size()+" OffHeap size="+mCache.size()+" Memory="+mCache.getTotalAllocatedMemorySize());
//		
//	}
//
//	/**
//	 * Inits the cache infinispan koda.
//	 *
//	 * @throws NativeMemoryException the native memory exception
//	 * @throws KodaException the koda exception
//	 */
//	private  void initCacheInfinispanKoda() throws NativeMemoryException, KodaException {
//		LOG.info("Initializing ISPN - Koda ...");
//		initCacheKoda();
//		OffHeapDataContainer dc = new OffHeapDataContainer(mCache);		
//		EmbeddedCacheManager manager = new DefaultCacheManager();						
//		Configuration config = new Configuration().fluent().
//									dataContainer().
//										dataContainer(dc).			    
//											build();
//
//		manager.defineConfiguration("name", config);
//		ispnCacheKoda  = manager.getCache("name");
//		LOG.info("Done");
//	}
//	
//	/**
//	 * Inits the cache kode.
//	 *
//	 * @throws NativeMemoryException the j emalloc exception
//	 * @throws KodaException the koda exception
//	 */
//	private void initCacheKoda() throws NativeMemoryException, KodaException {
//		com.koda.cache.CacheManager manager = com.koda.cache.CacheManager.getInstance();
//		com.koda.config.CacheConfiguration config = new com.koda.config.CacheConfiguration();
//		config.setBucketNumber(N);
//		config.setMaxMemory(sMemoryLimit);
//		config.setEvictionPolicy("LRU");
//		config.setDefaultExpireTimeout(6000);
//		config.setCacheName("test");
//		mCache = manager.createCache(config);
//
//	}
//	
//	
//	/**
//	 * Gets the key.
//	 * 
//	 * @param i
//	 *            the i
//	 * @param key
//	 *            the key
//	 * @return the key
//	 */
//	final byte[] get(int i, byte[] key) {
//		key[0] = (byte) (i >>> 24);
//		key[1] = (byte) (i >>> 16);
//		key[2] = (byte) (i >>> 8);
//		key[3] = (byte) (i);
//		return key;
//	}
//
//	/**
//	 * _test put remove.
//	 *
//	 * @throws NativeMemoryException the native memory exception
//	 * @throws IOException Signals that an I/O exception has occurred.
//	 */
//	public void _testPutRemove() throws NativeMemoryException, IOException
//	{
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);
//		byte[] value = new byte[500];
//		LOG.info("Starting filltest PutRemove cache");
//		//long t1 = System.currentTimeMillis();
//		for(int i=0; i < 1; i++){
//			byte[] bkey = get(i, key);
//			byte[] bvalue = get(i, value);
//			mCache.put(bkey, bvalue);
//		}
////		int nulls = 0;
////		for(int i=0; i < 10; i++){
////			byte[] bkey = get(i, key);
////			byte[] bvalue =(byte[]) mCache.get(bkey);
////			if(bvalue == null ) nulls++;
////		}
//		
////		assertEquals(0, nulls);
//		
//		//long t2 = System.currentTimeMillis();
//		LOG.info("Cache size ="+mCache.size());
//		
//		for(int i=0; i < 1; i++){
//			byte[] bkey = get(i, key);
//			//byte[] bvalue = get(i, value);
//			mCache.remove(bkey);
//		}
//		LOG.info("Size after remove="+mCache.size());
//		assertEquals(0, mCache.size());
//		
//	}
//	
//	/**
//	 * Test gets.
//	 */
//	public void testGets(){
//		LOG.info("Testing GETS");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);
//		Random r = new Random();
//		
//		for(int i=0; i < 100; i++){
//			int n = r.nextInt(N);
//			byte[] bkey = get(n, key);
//			byte[] value = (byte[]) ispnCacheKoda.get(bkey);
//			assertTrue(compare(bkey, value));
//		}
//		LOG.info("Testing GETS done");
//	}
//
//	/**
//	 * Compare.
//	 *
//	 * @param bkey the bkey
//	 * @param value the value
//	 * @return true, if successful
//	 */
//	private boolean compare(byte[] bkey, byte[] value) {
//		 return bkey[0] == value[0] && bkey[1] == value[1] && bkey[2] == value[2] && bkey[3] == value[3];
//	}
//
//	/**
//	 * Test key set iterator.
//	 */
//	@SuppressWarnings("unchecked")
//	public void testKeySetIterator()
//	{
//		LOG.info("Testing keySet iterator");
//		long t1 = System.currentTimeMillis();
//		Set<Object> set = ispnCacheKoda.keySet();
//		int counter =0;
//		int nulls = 0;
//		for(Object key: set){
//			counter++;
//			if(key == null) nulls++;
//		}
//		long t2 = System.currentTimeMillis();
//		LOG.info("Testing keySet iterator Done in "+(t2-t1)+" ms");
//		assertEquals(N, counter);
//		assertEquals(0, nulls);
//	}
//	
//	/**
//	 * Test values iterator.
//	 */
//	@SuppressWarnings("unchecked")
//	public void testValuesIterator()
//	{
//		LOG.info("Testing values iterator");
//		Collection<Object> values = ispnCacheKoda.values();
//		long t1 = System.currentTimeMillis();		
//		
//		int counter =0;
//		int nulls = 0;
//		for(Object value: values){
//			counter++;
//			if(value == null) nulls++;
//		}
//		long t2 = System.currentTimeMillis();
//		LOG.info("Testing values iterator Done in "+(t2-t1)+" ms");
//		assertEquals(N, counter);
//		assertEquals(0, nulls);
//	}
//	
//	/**
//	 * Test entry set iterator.
//	 */
//	@SuppressWarnings("unchecked")
//	public void testEntrySetIterator()
//	{
//		LOG.info("Testing entry set iterator");
//		Set<Map.Entry<?, ?>> entries = ispnCacheKoda.entrySet();
//		long t1 = System.currentTimeMillis();		
//		
//		int counter =0;
//		int key_nulls = 0;
//		int value_nulls =0;
//		int nulls =0;
//		for(Map.Entry<?,?> entry: entries){
//			counter++;
//			if(entry == null){
//				nulls++; continue;
//			}
//			if(entry.getKey() == null){
//				key_nulls++;
//			}
//			
//			if(entry.getValue() == null){
//				value_nulls ++;
//			}
//		}
//		long t2 = System.currentTimeMillis();
//		LOG.info("Testing entry set iterator Done in "+(t2-t1)+" ms");
//		assertEquals(N, counter);
//		assertEquals(0, nulls);
//		assertEquals(0, key_nulls);
//		assertEquals(0, value_nulls);
//	}
//	
//	/**
//	 * Test evict.
//	 */
//	@SuppressWarnings("unchecked")
//	public void testEvict()
//	{
//		LOG.info("Testing evict()");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);
//		int size = ispnCacheKoda.size();
//		//assertEquals(N, ispnCacheKoda.size());
//		
//		byte[] bkey = get(0, key);
//		ispnCacheKoda.evict(bkey);
//		assertEquals(size-1, ispnCacheKoda.size());
//		LOG.info("Testing evict() done.");
//	}
//	
//	/**
//	 * Test remove.
//	 */
//	public void testRemove()
//	{
//		LOG.info("Testing remove");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//
//		int size = ispnCacheKoda.size();
//		byte[] bkey = get(1, key);
//		assertNotNull(ispnCacheKoda.get(bkey));
//		assertNotNull(ispnCacheKoda.remove(bkey));
//		assertEquals(size-1, ispnCacheKoda.size());
//		assertNull(ispnCacheKoda.get(bkey));
//		LOG.info("Testing remove done.");
//	}
//	
//	/**
//	 * Test put if absent.
//	 */
//	@SuppressWarnings("unchecked")
//	public void testPutIfAbsent()
//	{
//		LOG.info("Testing put if absent");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(2, key);
//		byte[] bvalue = new byte[12];
//		// try to put 
//		byte[] value = (byte[])ispnCacheKoda.putIfAbsent(bkey, bvalue);
//		assertNotNull(value);
//		assertFalse(value.length ==bvalue.length);
//		bkey = get(0, key);
//		assertNull(ispnCacheKoda.get(bkey));
//		value = (byte[])ispnCacheKoda.putIfAbsent(bkey, bvalue);
//		assertNull(value);
//		value = (byte[]) ispnCacheKoda.get(bkey);
//		assertTrue(value.length == bvalue.length);
//		LOG.info("Testing put if absent  done.");
//	}
//	
//	/**
//	 * Test put for external read.
//	 */
//	@SuppressWarnings("unchecked")
//	public void testPutForExternalRead()
//	{
//		LOG.info("Testing put for external read");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(3, key);
//		byte[] bvalue = new byte[12];
//		// try to put 
//		ispnCacheKoda.putForExternalRead(bkey, bvalue);
//		byte[] value = (byte[])ispnCacheKoda.get(bkey);
//		assertNotNull(value);
//		assertTrue(value.length != bvalue.length);
//		
//		ispnCacheKoda.remove(bkey);
//		// try to put 
//		ispnCacheKoda.putForExternalRead(bkey, bvalue);
//		value = (byte[])ispnCacheKoda.get(bkey);
//		assertNotNull(value);
//		assertTrue(value.length == bvalue.length);
//		
//		LOG.info("Testing put for external read done.");
//	}
//	
//	/**
//	 * Test replace.
//	 */
//	@SuppressWarnings("unchecked")	
//	public void testReplace()
//	{
//		LOG.info("Testing replace");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(N+1, key);
//		byte[] bvalue = new byte[12];
//		// try to put 
//		ispnCacheKoda.replace(bkey, bvalue);
//		byte[] value = (byte[])ispnCacheKoda.get(bkey);
//		assertNull(value);
//		
//		bkey = get(N-1, key);
//		ispnCacheKoda.replace(bkey, bvalue);
//		value = (byte[])ispnCacheKoda.get(bkey);
//		assertNotNull(value);
//		assertTrue(value.length == bvalue.length);
//		
//		LOG.info("Testing replace done.");
//	}
//	
//	/**
//	 * Test get async.
//	 *
//	 * @throws InterruptedException the interrupted exception
//	 * @throws ExecutionException the execution exception
//	 */
//	@SuppressWarnings("unchecked")	
//	public void testGetAsync() throws InterruptedException, ExecutionException
//	{
//		LOG.info("Testing getAsync");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(N+1, key);
//
//		// try to put 
//		NotifyingFuture<?> future = ispnCacheKoda.getAsync(bkey);
//		
//		assertNull(future.get());
//		
//		bkey = get(N-1, key);
//		future = ispnCacheKoda.getAsync(bkey);
//		
//		assertNotNull(future.get());
//		
//		LOG.info("Testing getAsync done.");
//	}
//	
//	/**
//	 * Test put async.
//	 *
//	 * @throws InterruptedException the interrupted exception
//	 * @throws ExecutionException the execution exception
//	 */
//	@SuppressWarnings("unchecked")	
//	public void testPutAsync() throws InterruptedException, ExecutionException
//	{
//		LOG.info("Testing putAsync");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(N+2, key);
//		byte[] bvalue = new byte[12];
//		// try to put 
//		NotifyingFuture<?> future = ispnCacheKoda.putAsync(bkey, bvalue);
//		
//		future.get();
//		future = ispnCacheKoda.getAsync(bkey);
//		
//		assertNotNull(future.get());
//		
//		LOG.info("Testing putAsync done.");
//	}
//	
//	/**
//	 * Test put if absent async.
//	 *
//	 * @throws InterruptedException the interrupted exception
//	 * @throws ExecutionException the execution exception
//	 */
//	@SuppressWarnings("unchecked")	
//	public void testPutIfAbsentAsync() throws InterruptedException, ExecutionException
//	{
//		LOG.info("Testing putIfAbsentAsync");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(N+2, key);
//		byte[] bvalue = new byte[22];
//		// try to put 
//		NotifyingFuture<byte[]> future = ispnCacheKoda.putIfAbsentAsync(bkey, bvalue);
//		
//		assertNotNull(future.get());
//		assertTrue(future.get().length != bvalue.length);
//
//		bkey = get(N+3, key);
//		future = ispnCacheKoda.putIfAbsentAsync(bkey, bvalue);
//		assertNull(future.get());
//		
//		byte[] value = (byte[])ispnCacheKoda.get(bkey);
//		assertTrue(value.length == bvalue.length);
//		
//		LOG.info("Testing putIfAbsentAsync done." );
//	}
//	
//	/**
//	 * Test remove async.
//	 *
//	 * @throws InterruptedException the interrupted exception
//	 * @throws ExecutionException the execution exception
//	 */
//	@SuppressWarnings("unchecked")	
//	public void testRemoveAsync() throws InterruptedException, ExecutionException
//	{
//		LOG.info("Testing removeAsync");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(N+2, key);
//
//		// try to put 
//		NotifyingFuture<byte[]> future = ispnCacheKoda.removeAsync(bkey);
//		
//		assertNotNull(future.get());
//			
//		byte[] value = (byte[])ispnCacheKoda.get(bkey);
//		assertNull(value);
//		
//		LOG.info("Testing removeAsync done." );
//	}
//	
//	/**
//	 * Test replace async.
//	 *
//	 * @throws InterruptedException the interrupted exception
//	 * @throws ExecutionException the execution exception
//	 */
//	@SuppressWarnings("unchecked")	
//	public void testReplaceAsync() throws InterruptedException, ExecutionException
//	{
//		LOG.info("Testing replaceAsync");
//		String name = Thread.currentThread().getName();
//		byte[] key = new byte[4+ name.length()];
//		System.arraycopy(name.getBytes(), 0, key, 4, key.length-4);		
//		
//		byte[] bkey = get(N+4, key);
//		byte[] bvalue = new byte[222];
//		// try to replace 
//		NotifyingFuture<byte[]> future = ispnCacheKoda.replaceAsync(bkey, bvalue);
//		
//		bkey = get(N-2, key);
//		future = ispnCacheKoda.replaceAsync(bkey, bvalue);
//		assertNotNull(future.get());
//			
//		byte[] value = (byte[])ispnCacheKoda.get(bkey);
//		assertTrue(value.length == bvalue.length);
//		
//		LOG.info("Testing replaceAsync done." );
//	}
//}
//

