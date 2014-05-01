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

import junit.framework.TestCase;

import org.yamm.core.UnsafeMallocNoCompaction;
import org.yamm.util.Utils;

import sun.misc.Unsafe;

import com.koda.common.util.UnsafeAccess;

// TODO: Auto-generated Javadoc
/**
 * The Class UnsafeMallocNoCompactionTest.
 */
public class UnsafeMallocNoCompactionTest extends TestCase{

	
	/** The um. */
	static UnsafeMallocNoCompaction um = new UnsafeMallocNoCompaction();
	
	/**
	 * Test one.
	 */
	public void testOne(){
		System.out.println("Test: print malloc allocation sizes:");
		um.dumpSlabAllocationSizes();		
	}
	
	/**
	 * Test two.
	 */
	public void testTwo()
	{
		System.out.println("Test: simple malloc / free");
		long mem = um.malloc(1024);		
		um.free(mem);
		System.out.println("Allocd: "+UnsafeMallocNoCompaction.address(mem)+" mem="+mem);		
		
	}
	
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
	private long loadData(UnsafeMallocNoCompaction um, int N, int M, int seed, long[] mems)
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
			long real = UnsafeMallocNoCompaction.address(mems[i]);	
			for(int j=0; j < arr.length; j++) arr[j] = (byte)(real % 111);			
			unsafe.putInt(real, size);
			Utils.memcpy(arr, 0, arr.length, real +4);
		}
		return rawSize;
	}
	
	/**
	 * Test three.
	 */
	public void testThree(){
		System.out.println("Test: multiple alocations / copy / verification");
		Unsafe unsafe = UnsafeAccess.getUnsafe();
		int N = 100000;
		int M = 10000;
		long[] mems = new long[N];
		
		long rawSize = loadData(um, N, M, 0, mems);
		
		System.out.println("Raw Size="+rawSize+ " Allocated ="+um.memoryAllocated());
		
		System.out.println("Verification ...");
		
		for(int i=0; i < N ; i ++)
		{
			long real = UnsafeMallocNoCompaction.address(mems[i]);

			int size = unsafe.getInt(real);
			byte[] arr = new byte[size];
			Utils.memcpy(real+4, arr, 0, size);

			assertEquals(0, Utils.cmp(arr, (byte)(real % 111)));
		}
		
		System.out.println("Done.");
		
	}
	
	/**
	 * Test slab sizes.
	 */
	public void testSlabSizes(){
		
		System.out.println("Test: verify slab sizes ");
		long[] arr = new long[]{ 1,3,8, 11, 15, 16, 21,24, 25, 31, 38, 46, 48, 55, 60, 67,78, 87, 96, 107, 123, 138, 156, 176, 199, 211, 234, 256};
		
		for(int i=0; i < arr.length; i++){
			System.out.println(arr[i]+"-"+um.getSlabSize(arr[i]));
		}
		System.out.println("Done.");
	}
	
	/**
	 * Test slab allocations.
	 */
	public void testSlabAllocations()
	{
		System.out.println("Test: verify slab sizes ");
		um.dumpSlabAllocationSizes();
		System.out.println("Done.");
	}
	
	/**
	 * Test alloc free multiple.
	 */
	public void testAllocFreeMultiple()
	{
		System.out.println("Test: malloc/free multiple ");
		long startSize = um.memoryAllocated();
		int N = 100000;
		int M = 10000;
		long[] mems = new long[N]; 
		
		//um.freeListDump();
		
		@SuppressWarnings("unused")
    long rawSize = loadData(um, N, M, 1, mems);
		
		long newSize = um.memoryAllocated();
		
		System.out.println("+ Free "+ N+" objects");
		
		for(int i=0; i < N; i++) um.free(mems[i]);
		
		long afterFree = um.memoryAllocated();
		System.out.println("+ Load data again");
		
		loadData(um, N,M, 1, mems);
		
		System.out.printf("start=%d after_load=%d after_free=%d after_load=%d\n", startSize, newSize, afterFree, um.memoryAllocated());
		
		//um.freeListDump();
		
		System.out.println("+ #2: Free "+ N+" objects");
		
		for(int i=0; i < N; i++) um.free(mems[i]);
		
		afterFree = um.memoryAllocated();
		System.out.println("+ #2: Load data again");
		
		loadData(um, N,M, 1, mems);
		
		System.out.printf("+ #2: start=%d after_load=%d after_free=%d after_load=%d\n", startSize, newSize, afterFree, um.memoryAllocated());

		System.out.println("#3: Free "+ N+" objects");
		
		for(int i=0; i < N; i++) um.free(mems[i]);
		
		afterFree = um.memoryAllocated();
		System.out.println("+ #3: Load data again");
		
		loadData(um, N,M, 1, mems);
		
		System.out.printf("+ #3: start=%d after_load=%d after_free=%d after_load=%d\n", startSize, newSize, afterFree, um.memoryAllocated());
		
		System.out.println("Done.");
	}	
	
	/**
	 * Test large allocation.
	 */
	public void testLargeAllocation()
	{
		System.out.println("Test Large Object allocation");
		
		UnsafeMallocNoCompaction um = new UnsafeMallocNoCompaction(10000000);
		
		long ptr = um.malloc(5000000);
		assertTrue(ptr != 0);	
		//System.out.println("Usable Size="+um.mallocUsableSize(ptr));
		um.free(ptr);
		
		ptr = um.malloc(10000000);
		assertTrue( ptr == 0);
		//ptr = um.malloc
		System.out.println("Done.");
	}
	
	/**
	 * Test malloc usable size.
	 */
	public void testMallocUsableSize()
	{
		System.out.println("Test malloc usable size ");
		
		UnsafeMallocNoCompaction um = new UnsafeMallocNoCompaction(15000000);
		
		long ptr = um.malloc(5000000);
		assertTrue(ptr != 0);	
		System.out.println("Usable Size for Large - "+um.mallocUsableSize(ptr));
		um.free(ptr);
		
		ptr = um.malloc(100);
		assertTrue( ptr != 0);
		System.out.println("Usable Size for 100 - "+um.mallocUsableSize(ptr));
		
		ptr = um.malloc(1000);
		assertTrue( ptr != 0);
		System.out.println("Usable Size for 1000 - "+um.mallocUsableSize(ptr));
		
		//ptr = um.malloc
		System.out.println("Done.");
	}
}
