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
package com.koda.common.test;

import java.util.Random;

import junit.framework.TestCase;

import com.koda.common.util.MovingNumericHistogram;

// TODO: Auto-generated Javadoc
/**
 * The Class MovingNumericHistogramTest.
 */
public class MovingNumericHistogramTest extends TestCase
{
  
  /**
   * Test histogram.
   */
  public void testHistogram()
  {
    System.out.println("Test histogram  started. ");
    int M = 10;
    double [] arr = new double[1000000];
    Random r = new Random();
    for(int i=0; i < arr.length; i++){
      arr[i] = r.nextGaussian();
    }
    MovingNumericHistogram histogram = new MovingNumericHistogram(100, M);
    
    long start = System.currentTimeMillis();
   
    for(int i=0; i < M; i++){
      for(int j =0; j < arr.length; j++){
        histogram.add((i+1)*arr[j]);
      }
      histogram.advance();
      System.out.println("m="+i+"\n"+ histogram.toString(10));
    }
    long end = System.currentTimeMillis();
    
    System.out.println("Test histogram add perf finished. Time for "+(arr.length*M)+" ="+(end-start)+"ms");
  
  }
}
