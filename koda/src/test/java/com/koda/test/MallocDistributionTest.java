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

import java.nio.ByteBuffer;

import com.koda.NativeMemory;
import com.koda.NativeMemoryException;

// TODO: Auto-generated Javadoc
/**
 * The Class MallocDistributionTest.
 */
public class MallocDistributionTest {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws NativeMemoryException the native memory exception
	 */
	public static void main(String[] args) throws NativeMemoryException {
		
		
		int increment = 1;
		int start = 1;
		int stop = 1400;
		
		for(int size = start; size <=stop; size+=increment)
		{
			long ptr = NativeMemory.malloc(size);
			long allocated = NativeMemory.mallocUsableSize(ptr);
			System.out.println("size="+size+" allocated="+allocated);
			NativeMemory.free(ptr);
		}
		
		System.out.println("Started allocation / copy rate");
		int N = 20000, M = 100000;
		byte[] buf = new byte[N];
		for(int k=0; k < buf.length; k++)
		{
			buf[k] = (byte)k;
		}
		
		long startTime = System.currentTimeMillis();
		long value=0;
		for(int i=0; i < M; i++){
			ByteBuffer b = ByteBuffer.allocate(N);
			b.put(buf);
			if(b.get(10) != 10){
				System.err.println("abort");
				System.exit(0);
			}
			value += b.get(100);
		}
		long endTime = System.currentTimeMillis();
		System.out.println(M+" iterations took "+(endTime - startTime)+" ms. Value ="+value);

	}

}
