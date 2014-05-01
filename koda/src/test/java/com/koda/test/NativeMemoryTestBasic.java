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

import junit.framework.TestCase;

import org.apache.log4j.Logger;

import com.koda.NativeMemory;
import com.koda.NativeMemoryException;

// TODO: Auto-generated Javadoc
/**
 * The Class NativeMemoryTestBasic.
 */
public class NativeMemoryTestBasic extends TestCase {
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(NativeMemoryTestBasic.class);
	
		
	
	/**
	 * Test string ops.
	 *
	 * @throws NativeMemoryException the native memory exception
	 */
	public void testStringOps() throws NativeMemoryException
	{
		String s = "123456789utoiutre";
		ByteBuffer buf = NativeMemory.allocateDirectBuffer(16, s.length()*2);
		long ptr = NativeMemory.getBufferAddress(buf);
		LOG.info("Test String Ops started");
		NativeMemory.memcpy(s, 0, s.length(), ptr, 0);
		String ss = NativeMemory.memread2s(ptr);
		assertEquals(s, ss);
		LOG.info("Done. s ="+s+" ss="+ss);
	}
	

}
