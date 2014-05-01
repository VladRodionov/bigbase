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

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTableInterface;

public class TestCoprocessorEnvironment implements CoprocessorEnvironment{

  Configuration config ;
  public TestCoprocessorEnvironment(Configuration cfg){
    this.config = cfg;
  }
  
  @Override
  public Configuration getConfiguration() {
    // TODO Auto-generated method stub
    return config;
  }

  @Override
  public String getHBaseVersion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Coprocessor getInstance() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getLoadSequence() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getPriority() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HTableInterface getTable(byte[] tableName) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getVersion() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public HTableInterface getTable(byte[] arg0, ExecutorService arg1)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

}
