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
package com.koda.integ.hbase.stub;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;

// TODO: Auto-generated Javadoc
/**
 * The Class ByteArrayCacheable.
 */
public class ByteArrayCacheable implements Cacheable {

	/** The data. */
	byte[] data;
	
	/**
	 * Instantiates a new byte array cacheable.
	 *
	 * @param array the array
	 */
	public ByteArrayCacheable(byte[] array)
	{
		this.data = array;
	}
		
	
	/**
	 * Array.
	 *
	 * @return the byte[]
	 */
	public byte[] array()
	{
		return data;
	}
	
	/** The deserializer. */
	public static  CacheableDeserializer<Cacheable> deserializer = new CacheableDeserializer<Cacheable>(){

		@Override
		public Cacheable deserialize(ByteBuffer b) throws IOException {
			int size = b.getInt();
			byte[] data = new byte[size];
			b.get(data);
			return new ByteArrayCacheable(data);
		}
		
	};
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.io.hfile.Cacheable#getBlockType()
	 */
	@Override
	public BlockType getBlockType() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.io.hfile.Cacheable#getDeserializer()
	 */
	@Override
	public CacheableDeserializer<Cacheable> getDeserializer() {
		return ByteArrayCacheable.deserializer;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.io.hfile.Cacheable#getSchemaMetrics()
	 */
	@Override
	public SchemaMetrics getSchemaMetrics() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.io.hfile.Cacheable#getSerializedLength()
	 */
	@Override
	public int getSerializedLength() {
		return data.length + 4;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.io.hfile.Cacheable#serialize(java.nio.ByteBuffer)
	 */
	@Override
	public void serialize(ByteBuffer buf) {
		buf.putInt(data.length);
		buf.put(data);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.hbase.io.HeapSize#heapSize()
	 */
	@Override
	public long heapSize() {
		return 0;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object o)
	{
		if( o instanceof ByteArrayCacheable){
			ByteArrayCacheable bac = (ByteArrayCacheable) o;
			if( data.length != bac.data.length) return false;
			for(int i =0; i < data.length; i++)
			{
				if( data[i] != bac.data[i]) return false;
			}
			
			return true;
		}
		
		return false;
	}

}
