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
package com.koda.persistence;

// TODO: Auto-generated Javadoc
/**
 * The Class CacheEntry.
 */
public class CacheEntry {

	
	/** The key. */
	private Object key;
	
	/** The value. */
	private Object value;
	
	/** The expiration time. */
	private long expirationTime;
	
	/** The eviction data. */
	private int evictionData;
	
	/**
	 * Gets the key.
	 *
	 * @return the key
	 */
	public Object getKey() {
		return key;
	}
	
	/**
	 * Sets the key.
	 *
	 * @param key the new key
	 */
	public void setKey(Object key) {
		this.key = key;
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public Object getValue() {
		return value;
	}
	
	/**
	 * Sets the value.
	 *
	 * @param value the new value
	 */
	public void setValue(Object value) {
		this.value = value;
	}
	
	/**
	 * Gets the expiration time.
	 *
	 * @return the expiration time
	 */
	public long getExpirationTime() {
		return expirationTime;
	}
	
	/**
	 * Sets the expiration time.
	 *
	 * @param expirationTime the new expiration time
	 */
	public void setExpirationTime(long expirationTime) {
		this.expirationTime = expirationTime;
	}
	
	/**
	 * Gets the eviction data.
	 *
	 * @return the eviction data
	 */
	public int getEvictionData() {
		return evictionData;
	}
	
	/**
	 * Sets the eviction data.
	 *
	 * @param evictionData the new eviction data
	 */
	public void setEvictionData(int evictionData) {
		this.evictionData = evictionData;
	}
	
}
