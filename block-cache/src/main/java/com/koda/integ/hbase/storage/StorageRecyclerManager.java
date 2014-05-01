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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;


// TODO: Auto-generated Javadoc
/**
 * The Class StorageRecyclerManager.
 */
public class StorageRecyclerManager {

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(StorageRecyclerManager.class);
  
  /** The instance. */
  private static StorageRecyclerManager instance;
  
  /**
   * Gets the single instance of StorageRecyclerManager.
   *
   * @return single instance of StorageRecyclerManager
   */
  public static synchronized StorageRecyclerManager getInstance()
  {
    if(instance == null){
      instance = new StorageRecyclerManager();
    }
    return instance;
  }
  
  /**
   * Gets the storage recycler.
   *
   * @param config the config
   * @return the storage recycler
   */
  public StorageRecycler getStorageRecycler(Configuration config)
  {
    String implClass = 
      config.get(StorageRecycler.STORAGE_RECYCLER_IMPL, 
          "com.koda.integ.hbase.storage.FIFOStorageRecycler");
    
    try {
      Class<?> cls = Class.forName(implClass);
      StorageRecycler storageRec =  (StorageRecycler) cls.newInstance();
      
      return storageRec;
    } catch (Exception e) {
      LOG.fatal(implClass, e);
    }
    return null;  
  }
  
}
