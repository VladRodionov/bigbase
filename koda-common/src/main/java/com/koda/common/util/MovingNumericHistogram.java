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
package com.koda.common.util;

import java.util.ArrayList;


// TODO: Auto-generated Javadoc
/**
 * The Class MovingHistogram.
 */
public class MovingNumericHistogram {

  /** The data. */
  NumericHistogram[] data;  
  
  /** 
   * Cached moving histogram = all periods except current
   * It gets updated on next()
   * 
   **/
  NumericHistogram cached;
  
  /** The current index. */
  int currentIndex = 0;// works as a circular buffer
  
  /** The num bins. */
  int numBins ;
  
  /**
   * Instantiates a new moving histogram.
   *
   * @param numBins the num bins
   * @param historyLength the history length
   */
  public MovingNumericHistogram(int numBins, int historyLength)
  {
    this.numBins = numBins;
    data = new NumericHistogram[historyLength];
    for(int i = 0; i < historyLength; i++){
      data[i] = new NumericHistogram();
      data[i].allocate(numBins);
    }
  }
  
  /**
   * Gets the history length.
   *
   * @return the history length
   */
  public int getHistoryLength(){
    return data.length;
  }
  
  /**
   * Gets the num bins.
   *
   * @return the num bins
   */
  public int getNumBins(){
    return numBins;
  }
  
  /**
   * Finish current and advance index by 1..
   */
  public synchronized void advance()
  {
    currentIndex = (currentIndex + 1) % data.length;
    // reset current
    reset();
    // Update current cached merged histogram
    updateCachedMerged();
  }
  
  private void updateCachedMerged() {
    // Its called in synchronized block
    cached = new NumericHistogram();
    cached.allocate(numBins);
    for(int i= 1; i < data.length; i++){
      int index = currentIndex - i;
      if( index < 0) index = data.length + index;
      ArrayList<Double> serde = data[index].serialize();
      cached.merge(serde);
    }   
  }

  /**
   * Use this to update current active histogram.
   * The load is not going to be very high and we can afford using
   * standard Java synchronization.
   *
   * @param i the i
   * @param add the add
   * @return current value of a i-th bin
   */
  public synchronized void add(double add)
  {
    data[currentIndex].add(add);
  }
  
  
  /**
   * Reset.
   */
  private void reset() {
    
    data[currentIndex].reset();
    data[currentIndex].allocate(numBins);            
  }

  
  /**
   * Gets an approximate quantile value from the moving histogram. Some popular
   * quantiles are 0.5 (median), 0.95, and 0.98.
   *
   * @param q The requested quantile, must be strictly within the range (0,1).
   * @return The quantile value.
   */
  public double quantile(double q)
  {
    final NumericHistogram hist = cached != null? cached: data[currentIndex];
    return hist.quantile(q);
  }
  
  
  public double[] quantiles(double[] qs)
  {
    final NumericHistogram hist = cached != null? cached: data[currentIndex];
    return hist.quantiles(qs);
  }
  
  
  public String toString(int bins)
  {
    return cached != null? cached.toString(bins): data[currentIndex].toString(bins);
  }
 
  
}
