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
package com.koda.integ.hbase.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

// TODO: Auto-generated Javadoc
/**
 * The Class BlockCacheLoadReadScanTest.
 */
public class BlockCacheLoadReadScanTest extends BlockCacheBaseTest{

  /** The Constant LOG. */
  static final Log LOG = LogFactory.getLog(BlockCacheLoadReadScanTest.class);
  
  /**
   * Test sequential read.
   *
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void testSequentialRead() throws IOException
  {
    LOG.error("Sequential read starts. Reading "+N+" records.");
    long start = System.currentTimeMillis();
    int nulls = 0;
    for(int i=0; i < N; i++){
      Get get = createGet(i);
      Result r = _tableA.get(get);
      if(r.isEmpty()) nulls++;
    }
    LOG.error("Sequential read finished in "+(System.currentTimeMillis() - start)+"ms. Found nulls ="+nulls);

  }
  
  /**
   * Test random read.
   */
  public void testRandomRead()
  {
    
  }
  
  /**
   * Test full scanner.
   */
  public void testFullScanner()
  {
    
  }
  
  /**
   * Test random scanners.
   */
  public void testRandomScanners()
  {
    
  }
  
}
