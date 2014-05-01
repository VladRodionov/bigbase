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
package com.koda.persistence;

import sun.net.ProgressEvent;

// TODO: Auto-generated Javadoc
/**
 * The listener interface for receiving progress events.
 * The class that is interested in processing a progress
 * event implements this interface, and the object created
 * with that class is registered with a component using the
 * component's <code>addProgressListener<code> method. When
 * the progress event occurs, that object's appropriate
 * method is invoked.
 *
 * @see ProgressEvent
 */
public interface ProgressListener {

	/**
	 * Operation started.
	 */
	public void started();
	
	/**
	 * Report load/store operation progress.
	 *
	 * @param done - total rows loaded/stored
	 * @param total - total rows in store (if -1, then - undefined)
	 */
	public void progress(long done, long total);
	
	/**
	 * Report error.
	 *
	 * @param t - error
	 * @param aborted - if true operation was aborted
	 */
	public void error(Throwable t, boolean aborted);
	
	/**
	 * Operation finished successfully.
	 */
	public void finished();
	
	/**
	 * Operation cancelled.
	 */
	public void canceled();
	
	
}
