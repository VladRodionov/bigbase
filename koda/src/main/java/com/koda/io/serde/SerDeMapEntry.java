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

// TODO: Auto-generated Javadoc
/**
 * The Class SerDeMapEntry.
 */
class SerDeMapEntry {

	
	/** The m id. */
	private int mID;
	
	/** The m class. */
	private Class<?> mClass;
	
	/** The m serializer. */
	@SuppressWarnings("unchecked")
  private Serializer mSerializer;
	
	/**
	 * Instantiates a new triple value.
	 *
	 * @param id the id
	 * @param clz the clz
	 * @param serde the serde
	 */
	@SuppressWarnings("unchecked")
  public SerDeMapEntry(int id, Class<?> clz, Serializer serde)
	{
		this.mID = id;
		this.mClass = clz;
		this.mSerializer = serde;
	}

	/**
	 * Gets the iD value.
	 *
	 * @return the iD value
	 */
	public int getIDValue() {
		return mID;
	}

	/**
	 * Gets the class value.
	 *
	 * @return the class value
	 */
	public Class<?> getClassValue() {
		return mClass;
	}

	/**
	 * Gets the serializer value.
	 *
	 * @return the serializer value
	 */
	@SuppressWarnings("unchecked")
  public Serializer getSerializerValue() {
		return mSerializer;
	}
	
	
}
