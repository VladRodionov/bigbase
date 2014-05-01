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

import org.yamm.core.ConcurrentMalloc;
import org.yamm.core.MallocFactory;
import org.yamm.core.MallocWithCompactionFactory;
import org.yamm.core.MemoryPointerList;
import org.yamm.util.Utils;

import sun.misc.Unsafe;

import com.koda.common.util.UnsafeAccess;

// TODO: Auto-generated Javadoc
/**
 * The Class ThreadLocalStressTest.
 */
public class ConcurrentMallocStressTest
{
	
	/** The unsafe. */
	static Unsafe unsafe = UnsafeAccess.getUnsafe();

	
	/**
	 * Gets the value.
	 *
	 * @param n the n
	 * @param v the v
	 * @return the value
	 */
	private static byte[] getValue(int n, byte v){
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
	private static byte[] readValue(long memory)
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
	private static void writeValue(byte[] buf, long memory)
	{
		unsafe.putInt(memory, buf.length);
		Utils.memcpy(buf, 0, buf.length, memory+4);
	}

	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args){
		int numThreads = 4;
		int concurrencyLevel = 4;
		final long MAX_MEMORY = 100000000;
		MallocFactory factory = MallocWithCompactionFactory.getInstance(16, 64, 1.2D, MAX_MEMORY);
		final ConcurrentMalloc um = new ConcurrentMalloc(factory, concurrencyLevel);
        um.setCompactionActive(true);
		
		Runnable r = new Runnable(){
			public void run(){
				System.out.println(Thread.currentThread().getName()+" started.");

				long rawSize = 0;
				
				long TIME = 3600*1000; // 1 Hour
				//int MAX_SIZE = 1000;
                int[] OBJ_SIZES = new int[] { 200, 1000 };
                int sizeIndex = 0;
				int N = 10000000;
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
						boolean free = list.size() > 300000 && r.nextBoolean();
						if(free  || rawSize > MAX_MEMORY){

                            long targetSize = list.size() / 2;
                            while (list.size() > targetSize) {

                                long ptr = list.get();

                                // Lock malloc
                                long mem = um.lockAddress(ptr);
                                // Read data
                                @SuppressWarnings("unused")
                                byte[] v = readValue(mem);
                                //Unlock malloc
                                um.unlockAddress(ptr);

                                //if(0 != Utils.cmp(v, (byte)(ptr  % 111))){
                                //	System.err.println("ERROR");
                                //}
                                rawSize -= um.mallocUsableSize(ptr);
                                //*DEBUG*/System.out.println("free "+ptr);
                                um.free(ptr);
                                f++;

                            }
                            sizeIndex = ++sizeIndex % OBJ_SIZES.length;
						} else{

							// allocate new one
							///int size = r.nextInt(MAX_SIZE) + 1;
                            int size = OBJ_SIZES[sizeIndex];
							//System.out.println(Thread.currentThread().getName()+" before");
							long ptr = um.malloc(size + 4); 
							//if(ptr == 0)
							//*DEBUG*/System.out.println(Thread.currentThread().getName()+":malloc "+ptr);
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
					System.out.println(Thread.currentThread().getName()+":"+getTime(start)+" Allocd: "+um.memoryAllocated() +" time for "+N+" ops ="+(System.currentTimeMillis() -t)+"ms." +
							" failed="+totalFailedAllocations);
				}				
				
				System.out.println(Thread.currentThread().getName()+" finished.");
			}
		};
		
		Thread[] workers = new Thread[numThreads];
		for(int i=0; i < numThreads; i++){
			workers[i] = new Thread(r, "Worker #"+(i+1));
			workers[i].start();
		}
		
		for(int i=0; i< numThreads; i++){
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * Gets the time.
	 *
	 * @param start the start
	 * @return the time
	 */
	private static String getTime(long start) {
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
	private static String f(long s) {
		if(s < 10) return "0"+s;
		return s +"";
	}
}


