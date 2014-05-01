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

import com.koda.NativeMemory;

// TODO: Auto-generated Javadoc
/**
 * The Class NativeMemoryTest.
 */
public class NativeMemoryTest {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		System.out.println("Start");
		int N = 1000000;
		long t1 = System.currentTimeMillis();
		long[] ptrs = new long[N];
		byte[] buf = new byte[100];
		try{
			for(int i=0; i < N ; i++){
				ptrs[i] = NativeMemory.posix_memalign(16, 100);
				//System.out.println(ptrs[i]);
				NativeMemory.memcpy(buf,0, 100, ptrs[i],0);
				//NativeMemory.free(address);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		long t2 = System.currentTimeMillis();
		System.out.println("Put in "+(t2-t1)+" ms");	
		
		try{
			for(int i=0; i < N ; i++){
				NativeMemory.memcpy(ptrs[i],0, buf,0, 100);
				//NativeMemory.free(address);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		long t3 = System.currentTimeMillis();
		System.out.println("Get in "+(t3-t2)+" ms");
		try{
			for(int i=0; i < N ; i++){
				NativeMemory.free(ptrs[i]);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		long t4 = System.currentTimeMillis();
		System.out.println("Free in "+(t4-t3)+" ms");
		
	}

}
