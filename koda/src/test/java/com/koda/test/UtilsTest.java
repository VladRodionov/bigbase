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

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.NativeMemoryException;

public class UtilsTest extends TestCase {

    /** The Constant LOG. */
    private final static Logger LOG = Logger.getLogger(CompressionTest.class);
    
    

    
    public void testHash128() throws NativeMemoryException
    {
        LOG.info("Test 128bit hash");
//        ByteBuffer buf = ByteBuffer.allocateDirect(2048);
//        byte[] hash = new byte[16];
//        byte[] key = "1234567890".getBytes();
//        buf.put(key);
//        Utils.hash_murmur3_128(buf, 0, key.length, 0, hash);
//        for(int i=0; i < hash.length; i++){
//            System.out.print(hash[i]+" ");
//        }
//        LOG.info("\nTest 128bit hash finished");
//        long start = System.nanoTime();
//        for(int i =0; i < 10000000; i++)
//        {
//            hash[i%16] = (byte)(i%256);
//            Utils.hash_murmur3_128(buf, 0, key.length, 0, hash);
//        }
//        long end = System.nanoTime();
//        LOG.info("Time for 10M 128bit hash="+(end -start)/1000000+ " ms");
//        start = System.nanoTime();
//        long ptr = NativeMemory.getBufferAddress(buf);
//        for(int i =0; i < 10000000; i++)
//        {
//            hash[i%16] = (byte)(i%256);
//            Utils.hash_murmur3_128(ptr, 0, key.length, 0, hash);
//        }
//        end = System.nanoTime();
//        LOG.info("Time for 10M 128bit ptr hash="+(end -start)/1000000+ " ms");
//
//        start = System.nanoTime();
//        ByteBuffer out = ByteBuffer.allocateDirect(16);
//        long outptr = NativeMemory.getBufferAddress(out);
//        for(int i =0; i < 10000000; i++)
//        {
//            hash[i%16] = (byte)(i%256);
//            Utils.hash_murmur3_128(ptr, 0, key.length, 0, outptr);
//        }
 //       end = System.nanoTime();
 //       LOG.info("Time for 10M 128bit ptr-ptr hash="+(end -start)/1000000+ " ms");
    }
    
}
