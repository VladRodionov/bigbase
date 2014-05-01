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

// TODO: Auto-generated Javadoc
/**
 * The Class ThreadLocalTest.
 */
public class ThreadLocalTest {

	/** The s tls. */
	static ThreadLocal<Integer> sTLS = new ThreadLocal<Integer>();
	
	/** The N. */
	static int N = 1000000;
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		sTLS.set(0);
		long t1 = System.nanoTime();
		
		for(int i=0; i < N; i++)
		{
			Integer k = sTLS.get();
			sTLS.set(k);
		}
		
		long t2 = System.nanoTime();
		
		System.out.println("Time for "+N+" get/set ="+(t2-t1)/1000 +" microsecs");
		
	}

}
