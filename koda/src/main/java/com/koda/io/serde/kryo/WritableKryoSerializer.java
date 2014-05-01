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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.koda.io.serde.Writable;


// TODO: Auto-generated Javadoc
/**
 * The Class WritableSerializer extends KryoSerializer and support
 * serde ops for Writable objects.
 */
public class WritableKryoSerializer extends Serializer {

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(WritableKryoSerializer.class);
	
	/** The m kryo. */
	private Kryo mKryo;
	
	/** The s instance. */
	private static WritableKryoSerializer sInstance;
	
	/**
	 * Gets the single instance of WritableSerializer.
	 *
	 * @return single instance of WritableSerializer
	 */
	public static WritableKryoSerializer getInstance()
	{
		synchronized(WritableKryoSerializer.class){
			if(sInstance == null){
				sInstance = new WritableKryoSerializer();
			}
			return sInstance;
		}
	}
	
	/**
	 * Instantiates a new writable serializer.
	 */
	protected WritableKryoSerializer()
	{
		mKryo = new Kryo();
	}
	
	/* (non-Javadoc)
	 * @see com.esotericsoftware.kryo.Serializer#readObjectData(java.nio.ByteBuffer, java.lang.Class)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T readObjectData(ByteBuffer buffer, Class<T> type) {
		Writable obj = (Writable) newInstance(mKryo, type);
		try {
			obj.read(buffer);
		} catch (IOException e) {
			LOG.error(e);
			throw new RuntimeException(e); 
		}
		return (T)obj;
	}

	/* (non-Javadoc)
	 * @see com.esotericsoftware.kryo.Serializer#writeObjectData(java.nio.ByteBuffer, java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void writeObjectData(ByteBuffer buffer, Object obj) {
			try {
				((Writable)obj).write(buffer);
			} catch (IOException e) {
				LOG.error(e);
				throw new RuntimeException(e); 
			}

	}

}
