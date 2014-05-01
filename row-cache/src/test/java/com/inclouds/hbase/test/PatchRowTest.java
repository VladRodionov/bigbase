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
package com.inclouds.hbase.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;

// TODO: Auto-generated Javadoc
/**
 * The Class PatchRowTest.
 */
public class PatchRowTest extends BaseTest{
  	
	  /** The Constant LOG. */
	  static final Log LOG = LogFactory.getLog(PatchRowTest.class);
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#setUp()
	 */
	@Override
	protected void setUp()
	{
		// do nothing
	}
	
	/**
	 * Test key value patch.
	 */
	public void testKeyValuePatch()
	{
		LOG.info("KeyValue patch test started");
		KeyValue kv = 
			new KeyValue("000000".getBytes(), "family".getBytes(), "column".getBytes(), 0, "value".getBytes());
		
		LOG.info("Old row="+new String(kv.getRow()));
		patchRow(kv, "111".getBytes());
		LOG.info("New row="+new String(kv.getRow()));

		assertEquals(new String("111000".getBytes()), new String(kv.getRow()) );
		LOG.info("Finished OK");
		
	}
	
}
