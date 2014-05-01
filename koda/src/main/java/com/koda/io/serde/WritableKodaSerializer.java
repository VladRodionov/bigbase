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
 * The Class WritableKodaSerializer.
 *
 * @param <T> the generic type
 */
public class WritableKodaSerializer<T> implements Serializer<Writable<T>> {

	/** The clazz. */
	private Class<Writable<T>> clazz;
	
	/** The instance. */
	private Writable<T> instance;
	
	/**
	 * Instantiates a new writable koda serializer.
	 *
	 * @param cls the cls
	 */
	public WritableKodaSerializer(Class<Writable<T>> cls)
	{
		this.clazz = cls;
		try {
			this.instance = clazz.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassID(java.lang.Class)
	 */
	@Override
	public int getClassID(Class<?> clz) {		
		return instance.getID();
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassNamesAsList()
	 */
	@Override
	public List<String> getClassNamesAsList() {
		return Arrays.asList(new String[]{clazz.getName()});
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassesAsList()
	 */
	@Override
	public List<Class<?>> getClassesAsList() {
		return Arrays.asList(new Class<?>[]{clazz});
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
	public Writable<T> read(ByteBuffer buf) throws IOException {
		Writable<T> obj;
		try {
			obj = clazz.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new IOException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		
		obj.read(buf);
		return obj;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer, java.lang.Object)
	 */

	@Override
	public Writable<T> read(ByteBuffer buf, Writable<T> value) throws IOException {		
		Writable<T> var;
		var = (Writable<T>)value;
		var.read(buf);
		return var;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#write(java.nio.ByteBuffer, java.lang.Object)
	 */

	@Override
	public void write(ByteBuffer buf, Writable<T> obj) throws IOException {
		
		Writable<T> var;
		var = (Writable<T>)obj;
		var.write(buf);
	}

}
