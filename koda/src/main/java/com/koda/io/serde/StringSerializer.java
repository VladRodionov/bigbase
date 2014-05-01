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

import com.koda.NativeMemory;

// TODO: Auto-generated Javadoc
/**
 * The Class ByteArraySerializer.
 */
public class StringSerializer implements Serializer<String> {

	/** The m id. */
	private final static int ID = START_ID -2;
	
	/**
	 * Instantiates a new byte array serializer.
	 */
	public StringSerializer(){}
	
	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassID(java.lang.Class)
	 */
	@Override
	public int getClassID(Class<?> clz) {
		//if(clz.equals(byte[].class)){
		// Optimized for speed	
		return StringSerializer.ID;
		//} else{
			//throw new RuntimeException("Unexpected class: "+clz.getName());
		//}
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassesAsList()
	 */
	@Override
	public List<Class<?>> getClassesAsList() {		
		return Arrays.asList(new Class<?>[]{ String.class});
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassNamesAsList()
	 */
	@Override
	public List<String> getClassNamesAsList() {
		return Arrays.asList(new String[]{ String.class.getName()});
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
	public String read(ByteBuffer buf) throws IOException {
		long ptr;
		ptr = NativeMemory.getBufferAddress(buf);
		return NativeMemory.memread2s(ptr);
			
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public String read(ByteBuffer buf, String value) throws IOException {
		return read(buf);
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#write(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public void write(ByteBuffer buf, String obj) throws IOException {
		if(!(obj instanceof String)){
			throw new IOException("IllegalArgument type: "+obj);
		}
		String value = (String) obj;
		// This is byte array
		buf.putInt(value.length());
		long ptr;
		ptr = NativeMemory.getBufferAddress(buf);
		int off = buf.position();
		NativeMemory.memcpy(value, 0, value.length(), ptr, off);

	}

}
