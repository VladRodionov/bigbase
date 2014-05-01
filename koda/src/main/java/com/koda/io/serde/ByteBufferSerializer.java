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
package com.koda.io.serde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class ByteBufferSerializer.
 */
public class ByteBufferSerializer implements Serializer<ByteBuffer> {

	/** The Constant ID. */
	private final static int ID = START_ID -3;
	
	/** The Constant BIG_INDIAN. */
	private final static byte BIG_INDIAN = (byte)0;
	
	/** The Constant LITTLE_INDIAN. */
	private final static byte LITTLE_INDIAN = (byte)1;
	
	/** The Constant HEAP_BUFFER. */
	private final static byte HEAP_BUFFER = (byte)0;
	
	/** The Constant DIRECT_BUFFER. */
	private final static byte DIRECT_BUFFER = (byte)1;
	
	/**
	 * Instantiates a new byte buffer serializer.
	 */
	public ByteBufferSerializer(){}
	
	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassID(java.lang.Class)
	 */
	@Override
	public int getClassID(Class<?> clz) {
		return ByteBufferSerializer.ID;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassesAsList()
	 */
	@Override
	public List<Class<?>> getClassesAsList() {
		
		return Arrays.asList(new Class<?>[]{ ByteBuffer.class});
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#init()
	 */
	@Override
	public void init() {

	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer)
	 */
	@Override
	public ByteBuffer read(ByteBuffer buf) throws IOException {
		byte type = buf.get();
		byte order = buf.get();		
		int len = buf.getInt();
		int pos = buf.position();
		buf.limit(pos+len);
		ByteBuffer bbuf = (type == HEAP_BUFFER)?ByteBuffer.allocate(len): ByteBuffer.allocateDirect(len);
		bbuf.order((order == BIG_INDIAN)? ByteOrder.BIG_ENDIAN: ByteOrder.LITTLE_ENDIAN);
		bbuf.order(ByteOrder.nativeOrder());
		bbuf.put(buf);
		bbuf.flip();
		return bbuf;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public ByteBuffer read(ByteBuffer buf, ByteBuffer value) throws IOException {
		byte order = buf.get();
		int len = buf.getInt();
		int pos = buf.position();
		buf.limit(pos+len);
		ByteBuffer bbuf = (ByteBuffer) value;
		
		bbuf.order((order == BIG_INDIAN)? ByteOrder.BIG_ENDIAN: ByteOrder.LITTLE_ENDIAN);
		bbuf.position(0);
		bbuf.put(buf);
		return bbuf;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#write(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public void write(ByteBuffer buf, ByteBuffer obj) throws IOException
	{
		ByteBuffer bbuf = (ByteBuffer) obj;
		byte bufferType = bbuf.isDirect()? DIRECT_BUFFER: HEAP_BUFFER;
		ByteOrder order = bbuf.order();
		byte border = BIG_INDIAN;
		if(order == ByteOrder.LITTLE_ENDIAN)
		{
			border = LITTLE_INDIAN;
		}
		// write direct
		buf.put(bufferType);
		// write order
		buf.put(border);
		// write len		
		buf.putInt(bbuf.limit() - bbuf.position());
		buf.put(bbuf);
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassNamesAsList()
	 */
	@Override
	public List<String> getClassNamesAsList() {

		return Arrays.asList(new String[]{ "java.nio.HeapByteBuffer"});

	}

}
