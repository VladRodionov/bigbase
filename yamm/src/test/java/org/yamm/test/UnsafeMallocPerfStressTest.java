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
package org.yamm.test;

import java.util.Random;

import org.yamm.core.MemoryPointerList;
import org.yamm.core.UnsafeMalloc;
import org.yamm.util.Utils;

import sun.misc.Unsafe;

import com.koda.common.util.UnsafeAccess;

import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class UnsafeMallocPerfStressTest.
 */
public class UnsafeMallocPerfStressTest extends TestCase
{
	
	/** The um. */
	static UnsafeMalloc um = new UnsafeMalloc();
	

	
	/**
	 * Load data.
	 *
	 * @param um the um
	 * @param N the n
	 * @param M the m
	 * @param seed the seed
	 * @param mems the mems
	 * @return the long
	 */
	private long loadData(UnsafeMalloc um, int N, int M, int seed, long[] mems)
	{
		Unsafe unsafe = UnsafeAccess.getUnsafe();
		Random r = new Random(seed);
		 
		System.out.println("Allocate memory for "+N+" byte arrays of random sizes between 0 and "+M);
		long rawSize =0;
		for(int i = 0; i < N; i++ ){
			int size = r.nextInt(M) +M;
			byte[] arr = new byte[size];
			mems[i] =  um.malloc(size + 4); 
			rawSize += size+4;
			long real = UnsafeMalloc.address(mems[i]);	
			for(int j=0; j < arr.length; j++) arr[j] = (byte)1;//(real % 111);			
			unsafe.putInt(real, size);
			Utils.memcpy(arr, 0, arr.length, real +4);
		}
		return rawSize;
	}
		
	

	/**
	 * Test perf.
	 */
	public void testPerf()
	{
		System.out.println("Performance test");
		
		//um.freeListDump();
		
		int N = 10000;
		int M = 1000;
		int K = 10000;
		Random r = new Random();
		long[] mems = new long[N];
		long start = System.currentTimeMillis();
		for(int i=0; i < K; i++ )
			perfRound(mems, r, M);
		System.out.println("Time for "+(N*K) +" alloc/free = "+(System.currentTimeMillis() - start)+"ms");
		System.out.println("Final memory allocation ="+um.memoryAllocated());
	}

	/**
	 * Perf round.
	 *
	 * @param mems the mems
	 * @param r the r
	 * @param M the m
	 */
	private void perfRound(long[] mems, Random r, int M) {
		for(int i = 0; i < mems.length; i++){
			int size = r.nextInt(M) +1;
			mems[i] = um.malloc(size);
		}
		
		for(int i = 0; i < mems.length; i++){

			um.free(mems[i]);
		}
		
		
	}
	
	/**
	 * Test stress.
	 */
	public void testStress(){
		
		UnsafeMalloc um = new UnsafeMalloc(200000000);
		long[] mems = new long[100000];
		@SuppressWarnings("unused")
    long rawSize = loadData(um, 100000, 1000, 1, mems);
		
		long TIME = 3600*10; // 1 Hour
		int MAX_SIZE = 1000;
		int N = 100000000;
		MemoryPointerList list = new MemoryPointerList();
		
		for(int i =0; i < mems.length; i++) list.put(mems[i]);
		
		Random r = new Random();
		long start = System.currentTimeMillis();
		long f=0, m=0;
		
		long totalFailedAllocations = 0;
		while( System.currentTimeMillis() - start < TIME){
			
			// Do 1 M times
			int count = 0;
			long t = System.currentTimeMillis();
			while(count++ < N){
				boolean free = r.nextBoolean();
				if(free && list.isEmpty() == false){
					long ptr = list.get();
					um.free(ptr); f++;
				} else{
					//if(list.isEmpty()){
					//	System.out.println("Empty - size="+list.size());
					//}
					// allocate new one
					int size = r.nextInt(MAX_SIZE) + MAX_SIZE;
					long ptr = um.malloc(size); m++;
					if(ptr == 0L){
						totalFailedAllocations++;
					} else{
						list.put(ptr);
					}
				}
			}
			System.out.println(getTime(start)+" Allocd: "+um.memoryAllocated() +" time for "+N+" ops ="+(System.currentTimeMillis() -t)+"ms. Slabs="+um.getTotalMemoryBlocksAllocated()+
					" failed="+totalFailedAllocations+" rec="+um.getRecycledBlocks()+" totalAvailable="+um.totalAvailable());
		}
		
	}

	/**
	 * Gets the time.
	 *
	 * @param start the start
	 * @return the time
	 */
	private String getTime(long start) {
		long t = System.currentTimeMillis() - start;
		long h = t / (3600*1000);
		long m = (t % (3600*1000)) / (60*1000);
		long s = (t - h*3600*1000 - m*60*1000)/1000;
		return f(h)+":"+f(m)+":"+f(s);
	}

	/**
	 * F.
	 *
	 * @param s the s
	 * @return the string
	 */
	private String f(long s) {
		if(s < 10) return "0"+s;
		return s +"";
	}
	
	
}
