/*******************************************************************************
* Copyright (c) 2013, 2014 Vladimir Rodionov. All Rights Reserved
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.koda.integ.hbase.util.ConfigHelper;


import junit.framework.TestCase;


public class ConfigHelperTest extends TestCase{
	  /** The Constant LOG. */
	static final Log LOG = LogFactory.getLog(ConfigHelperTest.class);
	
	public void testConfigCopy()
	{
		LOG.info("Test config copy started");
		Configuration cfg1 = new Configuration();
		
		cfg1.set("Key1","Value1");
		cfg1.set("Key2","Value2");
		cfg1.set("Key3","Value3");
		
		Configuration cfg2 = ConfigHelper.copy(cfg1);
		assertTrue(cfg1.toString().equals(cfg2.toString()));
		LOG.info("Test config copy finished OK");

		
	}
}
