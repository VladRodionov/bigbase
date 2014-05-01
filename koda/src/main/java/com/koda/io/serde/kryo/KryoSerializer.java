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
package com.koda.io.serde.kryo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.koda.io.serde.Serializer;

// TODO: Auto-generated Javadoc
/**
 * The Class KryoSerializer.
 */
public class KryoSerializer implements Serializer<Object> {

	/** The m serializer. */
	Kryo mSerializer;
	
	/**
	 * Instantiates a new Kryo serializer.
	 */
	public KryoSerializer()
	{
		mSerializer = new Kryo();
		init();
	}
	
	/**
	 * Gets the kryo.
	 *
	 * @return the kryo
	 */
	public Kryo getKryo()
	{
		return mSerializer;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassID(java.lang.Class)
	 */
	@Override
	public int getClassID(Class<?> clz) {
		RegisteredClass rclz = mSerializer.getRegisteredClass(clz);
		if(rclz == null) throw new RuntimeException("class not registered: "+clz.getName());
		return rclz.getID();
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassesAsList()
	 */
	@Override
	public List<Class<?>> getClassesAsList() {
		throw new UnsupportedOperationException("too many to list");
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassNamesAsList()
	 */
	@Override
	public List<String> getClassNamesAsList() {
		throw new UnsupportedOperationException("too many to list");
	}

	
	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#init()
	 */
	@Override
	public void init() {
		mSerializer.register(byte[].class);
		//mSerializer.register(String.class);

	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer)
	 */
	@Override
	public Object read(ByteBuffer buf) throws IOException {
		
		return mSerializer.readClassAndObject(buf);
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public Object read(ByteBuffer buf, Object value) throws IOException {
		
		return mSerializer.readClassAndObject(buf);
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#write(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public void write(ByteBuffer buf, Object obj) throws IOException {
		mSerializer.writeClassAndObject(buf, obj);
	}

}
