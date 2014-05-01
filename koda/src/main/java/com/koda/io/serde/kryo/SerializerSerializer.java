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

import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Serializer;

// TODO: Auto-generated Javadoc
/**
 * The Class SerializerSerializer.
 */
public class SerializerSerializer extends Serializer {
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(SerializerSerializer.class);
	
	/** The m serializer. */
	private com.koda.io.serde.Serializer<Object> mSerializer;
	
	/**
	 * Instantiates a new serializer serializer.
	 *
	 * @param s the s
	 */
	public SerializerSerializer(com.koda.io.serde.Serializer<Object> s)
	{
		this.mSerializer = s;
	}
	
	/* (non-Javadoc)
	 * @see com.esotericsoftware.kryo.Serializer#readObjectData(java.nio.ByteBuffer, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T readObjectData(ByteBuffer buffer, Class<T> type) {
		
		try {
			return (T) mSerializer.read(buffer);
		} catch (IOException e) {
			LOG.error(e);
			throw new RuntimeException(e);
		}
	}

	/* (non-Javadoc)
	 * @see com.esotericsoftware.kryo.Serializer#writeObjectData(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public void writeObjectData(ByteBuffer buffer, Object object) {
		
		try {
			mSerializer.write(buffer, object);
		} catch (IOException e) {
			LOG.error(e);
			throw new RuntimeException(e);
		}
	}

}
