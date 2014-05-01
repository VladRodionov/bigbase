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

import org.yamm.core.MemoryPointerList;




// TODO: Auto-generated Javadoc
/**
 * The Class MemoryPointerListTest.
 */
public class MemoryPointerListTest /*extends TestCase*/ {

	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		MemoryPointerListTest test = new MemoryPointerListTest();
		test.testBasic();
		test.testMedium();
		test.testPartial();
	}
	
	/**
	 * Test basic.
	 */
	public void testBasic() 
	{
		int N = 100000;
		System.out.println("Start basic test. Put "+N+" values then read them back.");
		
		
		long[] data = new long [100000];
		for(int i=0; i < data.length; i++){
			data[i] = i+5;
		}
		
		MemoryPointerList list = new MemoryPointerList();
		for(int i =0; i < data.length; i++){
			list.put(data[i]);
		}
				
		
		System.out.println("Put "+data.length +" values"+" list size="+list.size() + 
				" blockSize="+list.blockSize()+" totalBlocks="+list.totalBlocks());
		
		for(int i =0; i < data.length; i++){
			if(data[i] != list.get()){
				System.out.println("ERROR "+i+": "+list.get());
			}
		}
		
		if(list.isEmpty() != true){
			System.out.println("ERROR - list no empty");
		}
		
		System.out.println("Reading done. Finished basic test");
	}
	
	/**
	 * Test medium.
	 */
	public void testMedium() 
	{
		int N = 100000;
		int M = 5;
		System.out.println("Start medium test. Put "+N+" values then read them back, repeat "+M+" times");
		
		
		long[] data = new long [100000];
		for(int i=0; i < data.length; i++){
			data[i] = i+5;
		}
		
		MemoryPointerList list = new MemoryPointerList();
		
		int counter =0;
		while( counter++ < M){
			for(int i =0; i < data.length; i++){
				list.put(data[i]);
			}
				
		
			System.out.println(counter+": Put "+data.length +" values"+" list size="+list.size() + 
				" blockSize="+list.blockSize()+" totalBlocks="+list.totalBlocks());
		
			for(int i =0; i < data.length; i++){
				if(data[i] != list.get()){
					System.out.println("ERROR "+i+": "+list.get());
				}
			}	
		
			if(list.isEmpty() != true){
				System.out.println("ERROR - list no empty");
			}	
		}
		
		System.out.println("Reading done. Finished medium test");
	}
	
	/**
	 * Test partial.
	 */
	public void testPartial() 
	{
		int N = 100000;
		int M = 5;
		System.out.println("Start partial test. Put "+N+" values then read them back (partially), repeat "+M+" times");
		
		
		long[] data = new long [100000];
		for(int i=0; i < data.length; i++){
			data[i] = i+5;
		}
		
		MemoryPointerList list = new MemoryPointerList();
		
		int counter =0;
		while( counter++ < M){
			for(int i =0; i < data.length; i++){
				list.put(data[i]);
			}
				
		
			System.out.println(counter+": Put "+data.length +" values"+" list size="+list.size() + 
				" blockSize="+list.blockSize()+" totalBlocks="+list.totalBlocks());
		
			for(int i =0; i < data.length/5; i++){
				if(data[i] != list.get()){
					//System.out.println("ERROR "+i+": "+list.get());
				}
			}	
		
		}
		
		System.out.println("Test done. Finished partial test Size="+list.size()+ " totalBlocks="+list.totalBlocks());
	}
	
}
