/*******************************************************************************
* Copyright (c) 2013, 2104 Vladimir Rodionov. All Rights Reserved
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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.koda.io.serde.ByteArraySerializer;

import junit.framework.TestCase;


public class ByteArraySerializerTest extends TestCase
{
	private final static Logger LOG = Logger
	.getLogger(ByteArraySerializerTest.class);
	
	public void testShortArray() throws IOException
	{
		ByteArraySerializer serde = new ByteArraySerializer();
		
		ByteBuffer buf = ByteBuffer.allocate(128);
		LOG.info("Size = 1");
		byte[] arr = new byte[1];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 1);
		buf.flip();
		byte[] result = serde.read(buf);		
		assertTrue(result.length == arr.length);
		
		LOG.info("Size = 10");
		buf.clear();
		arr = new byte[10];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 1);

		buf.flip();
		result = serde.read(buf);		
		assertTrue(result.length == arr.length);
		LOG.info("Size = 100");
		buf.clear();
		arr = new byte[100];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 1);

		buf.flip();
		result = serde.read(buf);		
		assertTrue(result.length == arr.length);		
		LOG.info("Size = 127");

		buf.clear();
		arr = new byte[127];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 1);

		buf.flip();
		result = serde.read(buf);		
		assertTrue(result.length == arr.length);

		
	}
	
	public void testLongArrays() throws IOException{
		ByteArraySerializer serde = new ByteArraySerializer();
		
		ByteBuffer buf = ByteBuffer.allocate(1000010);
		LOG.info("Size = 128");
		byte[] arr = new byte[128];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 4);

		buf.flip();
		byte[] result = serde.read(buf);		
		assertTrue(result.length == arr.length);
		
		LOG.info("Size = 1000");
		buf.clear();
		arr = new byte[1000];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 4);

		buf.flip();
		result = serde.read(buf);		
		assertTrue(result.length == arr.length);
		LOG.info("Size = 100000");
		buf.clear();
		arr = new byte[100000];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 4);

		buf.flip();
		result = serde.read(buf);		
		assertTrue(result.length == arr.length);		
		LOG.info("Size = 1000000");

		buf.clear();
		arr = new byte[1000000];
		serde.write(buf, arr);
		assertTrue(buf.position() == arr.length + 4);

		buf.flip();
		result = serde.read(buf);		
		assertTrue(result.length == arr.length);
	}
}