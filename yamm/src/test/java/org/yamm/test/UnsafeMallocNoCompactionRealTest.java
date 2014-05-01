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
import org.yamm.core.UnsafeMallocNoCompaction;
import org.yamm.util.Utils;

import sun.misc.Unsafe;

import com.koda.common.util.UnsafeAccess;

import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class UnsafeMallocPerfStressTest.
 */
public class UnsafeMallocNoCompactionRealTest extends TestCase
{
	
	/** The um. */
	static long MAX_MEMORY = 1000000000;
	
	/** The um. */
	static UnsafeMallocNoCompaction um = new UnsafeMallocNoCompaction(MAX_MEMORY);
	
	/** The unsafe. */
	static Unsafe unsafe = UnsafeAccess.getUnsafe();

	/**
	 * Gets the value.
	 *
	 * @param n the n
	 * @param v the v
	 * @return the value
	 */
	private byte[] getValue(int n, byte v){
		byte[] arr = new byte[n];
		Utils.memset(arr, v);
		return arr;
	}
	
	/**
	 * Read value.
	 *
	 * @param memory the memory
	 * @return the byte[]
	 */
	private byte[] readValue(long memory)
	{
		int size = Utils.getInt(memory);
		byte[] buf = new byte[size];
		Utils.memcpy(memory + 4, buf, 0, buf.length);
		return buf;
	}
	
	/**
	 * Write value.
	 *
	 * @param buf the buf
	 * @param memory the memory
	 */
	private void writeValue(byte[] buf, long memory)
	{
		unsafe.putInt(memory, buf.length);
		Utils.memcpy(buf, 0, buf.length, memory+4);
	}
	
	/**
	 * Test stress.
	 */
	public void testStress(){
				
		System.out.println("Stress test started");
		long rawSize = 0;
		
		long TIME = 3600*10; // 1 Hour
		int MAX_SIZE = 1000;
		int N = 100000000;
		MemoryPointerList list = new MemoryPointerList();
		
		Random r = new Random();
		long start = System.currentTimeMillis();
		long f=0, m=0;
		
		long totalFailedAllocations = 0;
		while( System.currentTimeMillis() - start < TIME){
			
			// Do 1 M times
			int count = 0;
			long t = System.currentTimeMillis();
			while(count++ < N){
				boolean free = list.size() > 1000000 && r.nextBoolean();
				if(free  || rawSize > MAX_MEMORY){
					long ptr = list.get();
					
					// Lock malloc
					long mem = um.lockAddress(ptr);
					// Read data
					byte[] v = readValue(mem);
					//Unlock malloc
					um.unlockAddress(ptr);
					
					assertEquals(0,Utils.cmp(v, (byte)(ptr  % 111)));
					rawSize -= um.mallocUsableSize(ptr);
					um.free(ptr); 					
					f++;
				} else{

					// allocate new one
					int size = r.nextInt(MAX_SIZE) + 1;
					long ptr = um.malloc(size + 4); 					
					m++;
					if(ptr == 0L){
						totalFailedAllocations++;
					} else{
						byte[] v = getValue(size, (byte)(ptr % 111));
						long mem = um.lockAddress(ptr);
						writeValue(v, mem);	
						um.unlockAddress(ptr);
						list.put(ptr);
						rawSize += um.mallocUsableSize(ptr);
					}
				}
			}
			System.out.println(getTime(start)+" Allocd: "+um.memoryAllocated() +" time for "+N+" ops ="+(System.currentTimeMillis() -t)+"ms. Slabs="+um.getTotalMemoryBlocksAllocated()+
					" failed="+totalFailedAllocations+" totalAvailable="+um.totalAvailable());
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
