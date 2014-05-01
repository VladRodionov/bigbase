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
 * The Enum FieldType.
 */
public enum FieldType {

	/** The BYTE. */
	BYTE, /** The UBYTE. */
 UBYTE, /** The SHORT. */
 SHORT, /** The USHORT. */
 USHORT, /** The INT. */
 INT, /** The UINT. */
 UINT, /** The LONG. */
 LONG, /** The FLOAT. */
 FLOAT, /** The DOUBLE. */
 DOUBLE;
	
	
	/**
	 * Size.
	 *
	 * @return the int
	 */
	public int size()
	{
		switch(ordinal()){
			case 0: return 1; 
			case 1: case 2: return 2;
			case 3: case 4: return 4;
			case 5: case 6: return 8;
			case 7: return 4;
			case 8: return 8;
		}
		return 0;
	}
}
