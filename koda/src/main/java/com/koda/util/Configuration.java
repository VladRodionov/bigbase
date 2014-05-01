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
package com.koda.util;

import java.util.HashMap;

// TODO: Auto-generated Javadoc
/**
 * The Class Configuration.
 */
public class Configuration {


	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -1874340101187261285L;
	
	/** The m properties. */
	public HashMap<String, String> mProperties;
	
	/**
	 * Instantiates a new configuration.
	 */
	public Configuration()
	{
		mProperties = new HashMap<String, String>();
	}
	
	/**
	 * Gets the.
	 *
	 * @param key the key
	 * @return the string
	 */
	public String get(String key){
		return mProperties.get(key);
	}

	/**
	 * Gets the.
	 *
	 * @param key the key
	 * @param defaultValue the default value
	 * @return the string
	 */
	public String get(String key, String defaultValue)
	{
		String v = mProperties.get(key);
		if(v != null) return v;
		return defaultValue;
	}
	
	/**
	 * Sets the.
	 *
	 * @param key the key
	 * @param v the v
	 */
	public void set(String key, String v)
	{
		mProperties.put(key, v);
	}
	
	/**
	 * Gets the boolean.
	 *
	 * @param key the key
	 * @param def the def
	 * @return the boolean
	 */
	public boolean getBoolean(String key, boolean def)
	{
		String v = mProperties.get(key);
		if(v != null) return Boolean.parseBoolean(v);
		return def;
	}
	
	/**
	 * Sets the boolean.
	 *
	 * @param key the key
	 * @param value the value
	 */
	public void setBoolean(String key, boolean value)
	{
		mProperties.put(key, Boolean.toString(value));
	}
	
	/**
	 * Gets the int.
	 *
	 * @param key the key
	 * @param defValue the def value
	 * @return the int
	 */
	public int getInt(String key, int defValue)
	{
		String v = mProperties.get(key);
		if(v != null) return Integer.parseInt(v);
		return defValue;
	}
	
	/**
	 * Sets the int.
	 *
	 * @param key the key
	 * @param value the value
	 */
	public void setInt(String key, int value)
	{
		mProperties.put(key, Integer.toString(value));
	}
	
	/**
	 * Gets the long.
	 *
	 * @param key the key
	 * @param defValue the def value
	 * @return the long
	 */
	public long getLong(String key, long defValue)
	{
		String v = mProperties.get(key);
		if(v != null) return Long.parseLong(v);
		return defValue;
	}
	
	/**
	 * Sets the long.
	 *
	 * @param key the key
	 * @param value the value
	 */
	public void setLong(String key, long value)
	{
		mProperties.put(key, Long.toString(value));
	}
	
}
