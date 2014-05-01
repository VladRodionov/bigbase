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

import junit.framework.TestCase;

import org.yamm.core.UnsafeMalloc;


// TODO: Auto-generated Javadoc
/**
 * The Class UnsafeMallocCompactionTest.
 */
public class UnsafeMallocCompactionTest extends TestCase{


	
	/**
	 * Test memory limit.
	 */
	public void testMemoryLimit()
	{
		System.out.println("\nTest memory limit");
		long MAX_MEMORY = 10000000;
		int SIZE = 1000;
		int N  = 20000;
		UnsafeMalloc um = new UnsafeMalloc(MAX_MEMORY);
		// let try all N
		long ptr = 0L;
		for(int i=0; i < N; i++){

			if( i == 10773){
			  System.out.println();
			}
		  ptr = um.malloc(SIZE);
			if(ptr == 0L){				
				System.out.println(i+" alloc returns 0L");
				break;
			}
		}		
		
		assertEquals(0L, ptr);
		System.out.println("Done.");
	}
	
	/**
	 * Test memory compaction full block.
	 */
	public void testMemoryCompactionFullBlock()
	{
		System.out.println("\nTest memory compaction full block");
		long MAX_MEMORY = 200000000;
		int SIZE = 1000;
		int SIZE_NEW = 2000;
		int N  = 200000;
		UnsafeMalloc um = new UnsafeMalloc(MAX_MEMORY);
		// let try all N
		//long ptr = 0L;
		long [] ptrs = new long[N];
		int n = 0;
		System.out.println("Allocate "+N +" objects of size "+SIZE+ " in "+MAX_MEMORY);
		for(int i=0; i < N; i++){

			ptrs[i] = um.malloc(SIZE);
			if(ptrs[i] == 0L) { n= i;
				System.out.println(i+" alloc returns 0L"); break;
			}
		}		
		System.out.println("Allocated "+ n +" objects of size "+SIZE);
		// now free all of them
		System.out.println("Free "+ n +" objects of size "+SIZE);
		for(int i=0; i < N; i++){

			if(ptrs[i] != 0L) {
				um.free(ptrs[i]); 
			} else{
				break;
			}

		}
		System.out.println("Try allocate "+N +" objects of size "+SIZE_NEW+ " in "+MAX_MEMORY);
		for(int i=0; i < N; i++){


			ptrs[i] = um.malloc(SIZE_NEW);
			if(ptrs[i] == 0L) { n= i;
				System.out.println(i+" alloc returns 0L"); break;
			}
		}	
		System.out.println("Allocated "+ n +" objects of size "+SIZE_NEW);
		//assertEquals(0L, ptr);
		// now free all of them
		
		
		System.out.println("Done.");
	}
	
	 /**
   * Test memory compaction full block.
   */
  public void testMemoryCompactionFullBlockReverse()
  {
    System.out.println("\nTest memory compaction full block: reverse");
    long MAX_MEMORY = 200000000;
    int SIZE = 2000;
    int SIZE_NEW = 1000;
    int N  = 100000;
    UnsafeMalloc um = new UnsafeMalloc(MAX_MEMORY);
    // let try all N
    //long ptr = 0L;
    long [] ptrs = new long[N];
    int n = 0;
    System.out.println("Allocate "+N +" objects of size "+SIZE+ " in "+MAX_MEMORY);
    for(int i=0; i < N; i++){

      ptrs[i] = um.malloc(SIZE);
      if(ptrs[i] == 0L) { n= i;
        System.out.println(i+" alloc returns 0L"); break;
      }
    }   
    System.out.println("Allocated "+ n +" objects of size "+SIZE);
    // now free all of them
    System.out.println("Free "+ n +" objects of size "+SIZE);
    for(int i=0; i < N; i++){

      if(ptrs[i] != 0L) {
        um.free(ptrs[i]); 
      } else{
        break;
      }

    }
    System.out.println("Try allocate "+N +" objects of size "+SIZE_NEW+ " in "+MAX_MEMORY);
    for(int i=0; i < N; i++){


      ptrs[i] = um.malloc(SIZE_NEW);
      if(ptrs[i] == 0L) { n= i;
        System.out.println(i+" alloc returns 0L"); break;
      }
    } 
    System.out.println("Allocated "+ n +" objects of size "+SIZE_NEW);
    //assertEquals(0L, ptr);
    // now free all of them
    
    
    System.out.println("Done.");
  }
  
	/**
	 * Test memory compaction partial block.
	 */
	public void testMemoryCompactionPartialBlock()
	{
		System.out.println("\nTest memory compaction partial block");
		long MAX_MEMORY = 200000000;
		int SIZE = 1000;
		int SIZE_NEW = 2000;
		int N  = 200000;
		UnsafeMalloc um = new UnsafeMalloc(MAX_MEMORY);
		// let try all N
		//long ptr = 0L;
		long [] ptrs = new long[N];
		int n = 0;
		System.out.println("Allocate "+N +" objects of size "+SIZE+ " in "+MAX_MEMORY+" slab size="+um.getSlabSize(SIZE));
		for(int i=0; i < N; i++){


			ptrs[i] = um.malloc(SIZE);
			if(ptrs[i] == 0L) { n= i;
				System.out.println(i+" alloc returns 0L"); break;
			}
		}		
		System.out.println("Allocated "+ n +" objects of size "+SIZE+" memory="+um.memoryAllocated());
		// now free all of them
		int blocks = (int)(n/(2*((um.blockSizeForSlab(SIZE) - 8)/um.getSlabSize(SIZE))));
		System.out.println("Free "+ n/2 +" (every second) objects of size "+SIZE+" total blocks ="+ blocks);
		for(int i=0; i < N; i++){

			if(ptrs[i] != 0L && ((i%2) == 0)) {
				um.free(ptrs[i]); 
			} else{
				//break;
			}

		}
		System.out.println("Try allocate "+N +" objects of size "+SIZE_NEW+ " in "+MAX_MEMORY+" slab size="+um.getSlabSize(SIZE_NEW));
		for(int i=0; i < N; i++){

			ptrs[i] = um.malloc(SIZE_NEW);
			if(ptrs[i] == 0L) { n= i;
				System.out.println(i+" alloc returns 0L"); break;
			}
		}	
		System.out.println("Allocated "+ n +" objects of size "+SIZE_NEW);
		System.out.println("Expected :" + blocks*((um.blockSizeForSlab(SIZE_NEW)-8)/um.getSlabSize(SIZE_NEW)));
		assertEquals(blocks*((um.blockSizeForSlab(SIZE_NEW)-8)/um.getSlabSize(SIZE_NEW)), n);
		
		
		//assertEquals(0L, ptr);
		System.out.println("Done.");
	}
	
	 /**
   * Test memory compaction partial block.
   */
  public void testMemoryCompactionPartialBlockReverse()
  {
    System.out.println("\nTest memory compaction partial block");
    long MAX_MEMORY = 200000000;
    int SIZE = 2000;
    int SIZE_NEW = 1000;
    int N  = 100000;
    UnsafeMalloc um = new UnsafeMalloc(MAX_MEMORY);
    // let try all N
    //long ptr = 0L;
    long [] ptrs = new long[N];
    int n = 0;
    System.out.println("Allocate "+N +" objects of size "+SIZE+ " in "+MAX_MEMORY+" slab size="+um.getSlabSize(SIZE));
    for(int i=0; i < N; i++){


      ptrs[i] = um.malloc(SIZE);
      if(ptrs[i] == 0L) { n= i;
        System.out.println(i+" alloc returns 0L"); break;
      }
    }   
    System.out.println("Allocated "+ n +" objects of size "+SIZE+" memory="+um.memoryAllocated());
    // now free all of them
    int blocks = (int)(n/(2*((um.blockSizeForSlab(SIZE) - 8)/um.getSlabSize(SIZE))));
    System.out.println("Free "+ n/2 +" (every second) objects of size "+SIZE+" total blocks ="+ blocks);
    for(int i=0; i < N; i++){

      if(ptrs[i] != 0L && ((i%2) == 0)) {
        um.free(ptrs[i]); 
      } else{
        //break;
      }

    }
    System.out.println("Try allocate "+N +" objects of size "+SIZE_NEW+ " in "+MAX_MEMORY+" slab size="+um.getSlabSize(SIZE_NEW));
    for(int i=0; i < N; i++){

      ptrs[i] = um.malloc(SIZE_NEW);
      if(ptrs[i] == 0L) { n= i;
        System.out.println(i+" alloc returns 0L"); break;
      }
    } 
    System.out.println("Allocated "+ n +" objects of size "+SIZE_NEW);
    System.out.println("Expected :" + blocks*((um.blockSizeForSlab(SIZE_NEW)-8)/um.getSlabSize(SIZE_NEW)));
    assertEquals(blocks*((um.blockSizeForSlab(SIZE_NEW)-8)/um.getSlabSize(SIZE_NEW)), n);
    
    
    //assertEquals(0L, ptr);
    System.out.println("Done.");
  }
  
	
	public void _testMulti()
	{
		for(int i =0; i < 10; i++){
			testMemoryCompactionPartialBlock();
		}
	}
	
}
