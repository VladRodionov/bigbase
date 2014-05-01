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
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Interface Serializer. This is a custom serialization support.
 * The special class which can handle SerDes for a particular list of classes
 * in a more efficient way than a standard (default) serializer.
 */
public interface Serializer<K> {

	/** The Constant START_ID. */
	public final static int START_ID = 1000000;
	
	/**
	 * Init serializer.
	 */
	public void init();
	
	/**
	 * Writes object into buffer.
	 *
	 * @param buf the buf
	 * @param obj the obj
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void write(ByteBuffer buf, K obj) throws IOException;
	
	/**
	 * Read object from buffer. Returns new instance.
	 *
	 * @param buf the buf
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public K read(ByteBuffer buf) throws IOException;
	
	/**
	 * Reads object from buffer into existing instance.
	 *
	 * @param buf the buf
	 * @param value the value
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public K read(ByteBuffer buf, K value) throws IOException;
	
	/**
	 * Gets the class id. Each class has its own ID
	 * which is assigned to it to minimize stream representation
	 * of a serialized object. 
	 * @param clz the class
	 * @return the class id
	 */
	public int getClassID(Class<?> clz);
	
	
	
	/**
	 * This is the optional method. Returns list of supported classes
	 * (classes which this serializer can handle). Default serializer
	 * (which is Kryo based) does not support this method as since it 
	 * MUST handle all classes except of:
	 *  1. Writables
	 *  2. classes which are supported by custom serializers.
	 *
	 * @return the classes as list
	 */
	public List<Class<?>> getClassesAsList();
	
	/**
	 * Gets the class names as list.
	 *
	 * @return the class names as list
	 */
	public List<String> getClassNamesAsList();
	
}
