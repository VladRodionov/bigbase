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

// TODO: Auto-generated Javadoc
/**
 * The Class RecordStructure.
 */
public class RecordStructure {

	/** The m types. */
	FieldType[] mTypes;
	
	/** The m names. */
	String[] mNames;

	/** The m offsets. */
	int [] mOffsets;
	
	/** The offset done. */
	boolean offsetDone = false;
	
	/** The size. */
	int size=-1;
	
	/**
	 * Instantiates a new record structure.
	 *
	 * @param size the size
	 */
	protected RecordStructure(int size)
	{
		mTypes = new FieldType[size];
		mNames = new String[size];
		mOffsets = new int[size];
	}
	
	/**
	 * Sets the type.
	 *
	 * @param i the i
	 * @param type the type
	 * @param name the name
	 */
	protected void setTypeName(int i, FieldType type, String name)
	{
		mTypes[i] = type;
		mNames[i] = name;
		
	}
	
	/**
	 * Gets the type.
	 *
	 * @param i the i
	 * @return the type
	 */
	public final FieldType getType(int i)
	{
		return mTypes[i];
	}
	
	/**
	 * Gets the offsets.
	 *
	 * @return the offsets
	 */
	public int[] getOffsets(){
		if(!offsetDone){
			calculateOffsets();
		}
		return mOffsets;
	}
	
	/**
	 * Calculate offsets.
	 */
	private void calculateOffsets() {
		for(int i=1; i < mTypes.length; i++)
		{
			mOffsets[i] = mOffsets[i-1]+ mTypes[i-1].size();
		}
		offsetDone = true;
	}

	
	/**
	 * Creates the from array.
	 *
	 * @param fieldTypes the field types
	 * @param fieldNames the field names
	 * @return the record structure
	 */
	public RecordStructure create(FieldType[] fieldTypes, String[] fieldNames)
	{
		RecordStructure structure = new RecordStructure(fieldTypes.length);
		for(int i=0; i < fieldTypes.length; i++){
			structure.setTypeName(i, fieldTypes[i], fieldNames[i]);
		}
		return structure;
	}

	
	/**
	 * Gets the field offset.
	 *
	 * @param name the name
	 * @return the field offset
	 */
	public final int getFieldOffset(final String name)
	{
		int index = find(name);
		if(index >= 0) return mOffsets[index];
		return -1;
	}
	
	/**
	 * Find.
	 *
	 * @param name the name
	 * @return the int
	 */
	private final int find(final String name) {
		for(int i=0; i< mNames.length; i++ )
		{
			if(mNames[i].equals(name)) return i;
		}
		return -1;
	}

	/**
	 * Gets the types.
	 *
	 * @return the types
	 */
	public FieldType[] getTypes() {

		return mTypes;
	}
	
	/**
	 * Gets the names.
	 *
	 * @return the names
	 */
	public String[] getNames()
	{
		return mNames;
	}
	
	/**
	 * Gets the size.
	 *
	 * @return the size
	 */
	public int getSize()
	{
		if(size < 0){
			size = 0;
			for(int i=0 ; i < mTypes.length; size+=mTypes[i++].size());
		}
		return size;
	}
}
