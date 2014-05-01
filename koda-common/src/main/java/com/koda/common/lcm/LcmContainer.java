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
package com.koda.common.lcm;

import java.io.Serializable;
import java.util.Properties;

/**
 * License container.
 * @author vrodionov
 *
 */
public class LcmContainer implements Serializable{

  Properties prop = new Properties();
  /**
   * 
   */
  private static final long serialVersionUID = -4455939243305872066L;
  
  public LcmContainer(){
    
  }
  
  public void setProperties(Properties p)
  {
    this.prop = p;
  }
  
  public void setProperty(String name, String value)
  {
    prop.setProperty(name, value);
  }
  
  public String getProperty(String name)
  {
    return prop.getProperty(name);
  }
  
  /**
   * Helper methods
   */
  public int getIntProperty(String name, int defValue)
  {
    String value = prop.getProperty(name);
    if (value == null) return defValue;
    return Integer.parseInt(value);
  }
  
  
  public long getLongProperty(String key, long defValue)
  {
    String value = prop.getProperty(key);
    if (value == null) return defValue;
    return Long.parseLong(value);
  }
  
  public boolean getBooleanProperty(String key, boolean defValue)
  {
    String value = prop.getProperty(key);
    if (value == null) return defValue;
    return Boolean.parseBoolean(value);
  
  }
  
  public String toString(){
    return prop.toString();
  }
  
  public boolean equals(Object obj){
    if(obj instanceof LcmContainer){
      return prop.equals(((LcmContainer)obj).prop);
    }
    return false;
  }
  
  
}
