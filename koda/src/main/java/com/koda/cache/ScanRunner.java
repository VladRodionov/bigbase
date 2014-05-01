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
package com.koda.cache;

import java.util.concurrent.Callable;

// TODO: Auto-generated Javadoc
/**
 * The Class ScanRunner.
 *
 * @param <T> the generic type
 */
public class ScanRunner<T> implements Callable<T>{

	/** The m scanner. */
	private CacheScanner mScanner;
	
	/** The m filter. */
	private Filter mFilter;
	
	/** The m scan. */
	private Scan<T> mScan;
	
	/**
	 * Instantiates a new scan runner.
	 *
	 * @param scanner the scanner
	 * @param filter the filter
	 * @param scan the scan
	 */
	public ScanRunner(CacheScanner scanner, Filter filter, Scan<T> scan)
	{
		this.mScanner = scanner;
		this.mFilter = filter;
		this.mScan = scan;
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.Callable#call()
	 */
	@Override
	public T call() throws Exception {
		
		long ptr =0;
		while((ptr = mScanner.nextPointer()) != 0L){
			if(mFilter == null || mFilter.filter(ptr)){
				mScan.scan(ptr);
			}
		}
		mScan.finish();
		return mScan.getResult();
	}
	
	
}
