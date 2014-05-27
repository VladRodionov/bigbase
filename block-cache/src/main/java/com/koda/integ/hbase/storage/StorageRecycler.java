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

// TODO: Auto-generated Javadoc
/**
 * The Interface StorageRecycler.
 */
public interface StorageRecycler {

  /** The Constant STORAGE_RECYCLER_IMPL. */
  public final static String STORAGE_RECYCLER_IMPL = "storage.recycler.class.impl";
  /** Storage low watermark. */ 
  public final static String STORAGE_RATIO_LOW_CONF     = "storage.recycler.low.watermark";  
  
  /** Storage high watermark. */  
  public final static String STORAGE_RATIO_HIGH_CONF    = "storage.recycler.high.watermark"; 
  
    
  
  /* Default values */
  /** Storage low watermark. */ 
  public final static String STORAGE_RATIO_LOW_DEFAULT     = "0.97f";  
  
  /** Storage high watermark. */  
  public final static String STORAGE_RATIO_HIGH_DEFAULT    = "0.98f"; 
  
  /**
   * Sets the.
   *
   * @param storage the storage
   */
  public void set(ExtStorage storage);
  
}
