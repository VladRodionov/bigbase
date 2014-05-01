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

import org.apache.log4j.Logger;

import com.koda.common.util.NumericHistogram;


import junit.framework.TestCase;

public class NumericHistogramTest extends TestCase
{
	@SuppressWarnings("unused")
  private final static Logger LOG = Logger.getLogger(NumericHistogramTest.class);

	public void testHistogramUniform()
	{
		System.out.println("Test histogram starts. Number of points: 10M. Timestamp data. Distribution is uniform.");
		int N = 1000000;
		int numBins = 100;
		long[] data = new long[N];
		// 7 days back
		long startTime  = System.currentTimeMillis() - ((long)7 * 24 * 3600 * 1000);
		System.out.println("START="+startTime);
		long endTime = System.currentTimeMillis();
		System.out.println("END  ="+ endTime);
		
		Random r = new Random();
		for(int i=0; i < N; i++){
			data[i] = (long)(r.nextDouble() *( endTime -startTime)) + startTime;
		}
		NumericHistogram histogram = new NumericHistogram();
		histogram.allocate(numBins);
		
		long start = System.currentTimeMillis();		
		for(int i=0 ; i < N; i++){
			histogram.add((double) data[i]);
		}		
		long end   = System.currentTimeMillis();
		System.out.println("Build time for "+N + " points ="+(end-start)+"ms");
		
		System.out.println("Median for "+((endTime+startTime)/2)+" = "+ (long)(histogram.quantile(0.5)));
		System.out.println("0.25 quantile for "+((endTime+3*startTime)/4)+" = "+ (long)(histogram.quantile(0.25)));
		System.out.println("0.75 quantile for "+((3*endTime+startTime)/4)+" = "+ (long)(histogram.quantile(0.75)));
		
		//System.out.println(histogram.toString(10));
		System.out.println("Test histogram finished. ");
	}
	
	
	public void testHistogramAddPerformance()
	{
	  System.out.println("Test histogram add perf started. ");
	  double [] arr = new double[1000000];
	  Random r = new Random();
	  for(int i=0; i < arr.length; i++){
	    arr[i] = r.nextGaussian();
	  }
	  NumericHistogram histogram = new NumericHistogram();
	  histogram.allocate(100);
	  
	  long start = System.currentTimeMillis();
	  int M = 10;
	  for(int i=0; i < M; i++){
	    for(int j =0; j < arr.length; j++){
	      histogram.add(arr[j]);
	    }
	  }
	  long end = System.currentTimeMillis();
	  
	  System.out.println("Test histogram add perf finished. Time for "+(arr.length*M)+" ="+(end-start)+"ms");
	}
}
