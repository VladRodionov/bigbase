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
package com.koda.io.record;

import com.koda.IOUtils;

// TODO: Auto-generated Javadoc
/**
 * Read / Write primitive types
 * from to native memory
 * Keep record structure.
 *
 * @author vrodionov
 */
public class RecordIO {
	
	/** The m structure. */
	final private RecordStructure mStructure;
	
	/** The m offsets. */
	final int[] mOffsets;
	
	/**
	 * Instantiates a new record io.
	 *
	 * @param struct the struct
	 */
	public RecordIO(RecordStructure struct)
	{
		this.mStructure = struct;
		mOffsets = struct.getOffsets();
	}
	
	/**
	 * Gets the type.
	 *
	 * @param index the index
	 * @return the type
	 */
	public final FieldType getType(int index)
	{
		return mStructure.getType(index);
	}
	
	/**
	 * Gets the types.
	 *
	 * @return the types
	 */
	public final FieldType[] getTypes()
	{
		return mStructure.getTypes();
	}
	
	/**
	 * Read byte.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the byte
	 */
	public final byte readByte(final long ptr, final int index)
	{
		return IOUtils.getByte(ptr, mOffsets[index]);
	}
	
	/**
	 * Read unsigned byte.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the short
	 */
	public final short readUByte(final long ptr, final int index)
	{
		return IOUtils.getUByte(ptr, mOffsets[index]);
	}
	
	
	/**
	 * Read short.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the short
	 */
	public final short readShort(final long ptr, final int index)
	{
		return IOUtils.getShort(ptr, mOffsets[index]);
	}
	
	/**
	 * Read unsigned short.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the int
	 */
	public final int readUShort(final long ptr, final int index)
	{
		return IOUtils.getUShort(ptr, mOffsets[index]);
	}
	
	/**
	 * Read int.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the int
	 */
	public final int readInt(final long ptr, final int index)
	{
		return IOUtils.getInt(ptr, mOffsets[index]);
	}
	
	/**
	 * Read unsigned int.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the long
	 */
	public final long readUInt(final long ptr, final int index)
	{
		return IOUtils.getUInt(ptr, mOffsets[index]);
	}
	
	/**
	 * Read long.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the long
	 */
	public final long readLong(final long ptr, final int index)
	{
		return IOUtils.getLong(ptr, mOffsets[index]);
	}

	/**
	 * Read float.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the float
	 */
	public final float readFloat(final long ptr, final int index)
	{
		return IOUtils.getFloat(ptr, mOffsets[index]);
	}

	/**
	 * Read double.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @return the double
	 */
	public final double readDouble(final long ptr, final int index)
	{
		return IOUtils.getDouble(ptr, mOffsets[index]);
	}
	
	
	/**
	 * Write byte.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeByte(final long ptr, final int index, final byte value)
	{
		IOUtils.putByte(ptr, mOffsets[index], value);
	}
	
	/**
	 * Write unsigned byte.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeUByte(final long ptr, final int index, final short value)
	{
		IOUtils.putUByte(ptr, mOffsets[index], value);
	}
	
	/**
	 * Write short.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeShort(final long ptr, final int index, final short value)
	{
		IOUtils.putShort(ptr, mOffsets[index], value);
	}
	
	/**
	 * Write unsigned short.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeUShort(final long ptr, final int index, final int value)
	{
		IOUtils.putUShort(ptr, mOffsets[index], value);
	}
	
	/**
	 * Write int.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeInt(final long ptr, final int index, final int value)
	{
		IOUtils.putInt(ptr, mOffsets[index], value);
	}
	
	/**
	 * Write unsigned int.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeUInt(final long ptr, final int index, final long value)
	{
		IOUtils.putUInt(ptr, mOffsets[index], value);
	}
	
	/**
	 * Write long.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeLong(final long ptr, final int index, final long value)
	{
		IOUtils.putLong(ptr, mOffsets[index], value);
	}
	
	
	/**
	 * Write float.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeFloat(final long ptr, final int index, final float value)
	{
		IOUtils.putFloat(ptr, mOffsets[index], value);
	}
	
	/**
	 * Write double.
	 *
	 * @param ptr the ptr
	 * @param index the index
	 * @param value the value
	 */
	public final void writeDouble(final long ptr, final int index, final double value)
	{
		IOUtils.putDouble(ptr, mOffsets[index], value);
	}
	
	
	
}
