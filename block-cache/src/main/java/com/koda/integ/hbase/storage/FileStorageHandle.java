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
package com.koda.integ.hbase.storage;

import com.koda.integ.hbase.util.Utils;

// TODO: Auto-generated Javadoc
/**
 * The Class FileStorageHandle.
 */
public class FileStorageHandle implements StorageHandle {

	/** The id. */
	private int id;
	
	/** The offset. */
	private int offset;
	
	/** The size. */
	private int size;
	
	/**
	 * Gets the id.
	 *
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * Sets the id.
	 *
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * Gets the offset.
	 *
	 * @return the offset
	 */
	public int getOffset() {
		return offset;
	}

	/**
	 * Sets the offset.
	 *
	 * @param offset the offset to set
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

	/**
	 * Gets the size.
	 *
	 * @return the size
	 */
	public int getSize() {
		return size;
	}

	/**
	 * Sets the size.
	 *
	 * @param size the size to set
	 */
	public void setSize(int size) {
		this.size = size;
	}

	/**
	 * Instantiates a new file storage handle.
	 */
	public FileStorageHandle(){}
	
	/**
	 * Instantiates a new file storage handle.
	 *
	 * @param id the id
	 * @param offset the offset
	 * @param size the size
	 */
	public FileStorageHandle(int id, int offset, int size)
	{
		this.id = id;
		this.offset = offset;
		this.size = size;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.StorageHandle#fromBytes(byte[])
	 */
	@Override
	public void fromBytes(byte[] arr) {
		id = Utils.toInt(arr, 0);
		offset = Utils.toInt(arr, 4);
		size = Utils.toInt(arr, 8);
	}

	/* (non-Javadoc)
	 * @see com.koda.integ.hbase.storage.StorageHandle#toBytes()
	 */
	@Override
	public byte[] toBytes() {
		byte[] bytes = new byte[12];
		Utils.toBytes(bytes, 0, id);
		Utils.toBytes(bytes, 4, offset);
		Utils.toBytes(bytes, 8, size);
		return bytes;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		FileStorageHandle fsh = (FileStorageHandle) obj;		
		return fsh.getId() == getId() && fsh.getOffset() == getOffset() && fsh.getSize() == getSize();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString(){
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("id="+id+" offset="+offset+" size="+size);
		return sbuf.toString();
	}
}
