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

import org.apache.log4j.Logger;


// TODO: Auto-generated Javadoc
/**
 * The listener interface for receiving defaultProgress events.
 * The class that is interested in processing a defaultProgress
 * event implements this interface, and the object created
 * with that class is registered with a component using the
 * component's <code>addDefaultProgressListener<code> method. When
 * the defaultProgress event occurs, that object's appropriate
 * method is invoked.
 *
 * @see DefaultProgressEvent
 */
public class DefaultProgressListener implements ProgressListener {

	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(DefaultProgressListener.class);
	
	/**
	 * Instantiates a new default progress listener.
	 */
	public DefaultProgressListener()
	{
		
	}
	
	/* (non-Javadoc)
	 * @see com.koda.persistence.ProgressListener#canceled()
	 */
	@Override
	public void canceled() {
		LOG.info("Canceled");
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.ProgressListener#error(java.lang.Throwable, boolean)
	 */
	@Override
	public void error(Throwable t, boolean aborted) {
		LOG.error("Error. Aborted="+aborted, t);
	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.ProgressListener#finished()
	 */
	@Override
	public void finished() {
		LOG.info("Finished");

	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.ProgressListener#progress(long, long)
	 */
	@Override
	public void progress(long done, long total) {
		LOG.info("Done "+Math.ceil( ( (double)done/total )* 100 )+"%");

	}

	/* (non-Javadoc)
	 * @see com.koda.persistence.ProgressListener#started()
	 */
	@Override
	public void started() {
		LOG.info("Started");

	}

}
