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
import java.util.Arrays;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class ByteArraySerializer.
 */
public class ByteArraySerializer implements Serializer<byte[]> {

	/** The m id. */
	private final static int ID = START_ID -1;
	
	/**
	 * Instantiates a new byte array serializer.
	 */
	public ByteArraySerializer(){}
	
	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassID(java.lang.Class)
	 */
	@Override
	public int getClassID(Class<?> clz) {
		return ID;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassesAsList()
	 */
	@Override
	public List<Class<?>> getClassesAsList() {		
		return Arrays.asList(new Class<?>[]{ byte[].class});
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#init()
	 */
	@Override
	public void init() {
		// do nothing

	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer)
	 */
	@Override
	public byte[] read(ByteBuffer buf) throws IOException {
		int pos = buf.position();
		// Read first byte
		int len = buf.get();
		if(len < 0){
			// Read 4 byte INT
			buf.position(pos);
			len = buf.getInt();
			len &= 0x7fffffff;
			
		} else{
			// SHORT array <= 127 bytes
		}
		byte[] buffer = new byte[len];
		buf.get(buffer);
		return buffer;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public byte[] read(ByteBuffer buf, byte[] value) throws IOException {
		if(!(value instanceof byte[]) ) 
		{
			throw new IOException("IllegalArgument type: "+value);
		}
		byte[] buffer = (byte[]) value;
		int pos = buf.position();
		// Read first byte
		int len = buf.get();
		if(len < 0){
			// Read 4 byte INT
			buf.position(pos);
			len = buf.getInt();
			len &= 0x7fffffff;

		} else{
			// SHORT array <= 127 bytes
		}
		if(len > buffer.length) {
			throw new IOException("Buffer underflow");
		}
		//TODO we have no idea how much data contained here
		buf.get(buffer, 0, len);
		return value;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#write(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public void write(ByteBuffer buf, byte[] obj) throws IOException {
		if(!(obj instanceof byte[])){
			throw new IOException("IllegalArgument type: "+obj);
		}
		byte[] value = (byte[]) obj;
		// This is byte array
		//buf.putInt(mID);
		if(value.length <= 127){
			buf.put((byte) value.length);
		} else{
			buf.putInt(value.length | (1 << 31));
		}
		buf.put(value);

	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassNamesAsList()
	 */
	@Override
	public List<String> getClassNamesAsList() {
		return Arrays.asList(new String[]{ byte[].class.getName()});
	}

}
