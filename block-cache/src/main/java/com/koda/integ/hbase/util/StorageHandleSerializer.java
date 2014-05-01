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
package com.koda.integ.hbase.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.koda.integ.hbase.storage.FileStorageHandle;
import com.koda.integ.hbase.storage.StorageHandle;
import com.koda.io.serde.Serializer;

// TODO: Auto-generated Javadoc
/**
 * The Class CacheableSerializer.
 */
public class StorageHandleSerializer implements Serializer<StorageHandle> {

	/** The id. */
	private static int ID = START_ID - 5;
	
	
	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassID(java.lang.Class)
	 */
	@Override
	public int getClassID(Class<?> clz) {
		return StorageHandleSerializer.ID;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassNamesAsList()
	 */
	@Override
	public List<String> getClassNamesAsList() {
		return Arrays.asList(new String[]{ 
				com.koda.integ.hbase.storage.StorageHandle.class.getName() });
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#getClassesAsList()
	 */
	@Override
	public List<Class<?>> getClassesAsList() {
		return Arrays.asList(new Class<?>[]{ 
				com.koda.integ.hbase.storage.StorageHandle.class});
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#init()
	 */
	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer)
	 */
	@Override
	public StorageHandle read(ByteBuffer buf) throws IOException {
		int size = buf.getInt();
		byte[] bytes = new byte[size];
		buf.get(bytes);
		// TODO: we support only FileStorageHandle
		FileStorageHandle fsh = new FileStorageHandle();
		fsh.fromBytes(bytes);
		return fsh;
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#read(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public StorageHandle read(ByteBuffer buf, StorageHandle value) throws IOException {
		return read(buf);
	}

	/* (non-Javadoc)
	 * @see com.koda.io.serde.Serializer#write(java.nio.ByteBuffer, java.lang.Object)
	 */
	@Override
	public void write(ByteBuffer buf, StorageHandle obj) throws IOException {
		byte[] bytes = obj.toBytes();
		buf.putInt(bytes.length);
		buf.put(bytes);
	}

}
