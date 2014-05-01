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
package com.koda.persistence.rawfs;

import org.apache.log4j.Logger;

import com.koda.config.DiskStoreConfiguration;


// TODO: Auto-generated Javadoc
/**
 * The Class RawFSConfiguration.
 */
public class RawFSConfiguration extends DiskStoreConfiguration {

	/** The Constant RW_BUFFER_SIZE. */
	public final static String RW_BUFFER_SIZE ="koda.rawfs.rw.buffersize";
	
	/** The Constant IO_THREADS. */
	public final static String IO_THREADS = "koda.rawfs.io.threads";
	
	/** The Constant WORKER_THREADS. */
	public final static String WORKER_THREADS = "koda.rawfs.worker.threads";
	
	
	/** The Constant LOG. */
	@SuppressWarnings("unused")
	private final static Logger LOG = Logger.getLogger(RawFSConfiguration.class);
	
	/** The Constant DEFAULT_BUFFER_SIZE. */
	protected final static int DEFAULT_BUFFER_SIZE= 4*1024*1024;
	
	/** The rw buffer size. */
	protected int rwBufferSize = DEFAULT_BUFFER_SIZE; 
	
	/** Snapshots interval in seconds. */
	protected int snapshotInterval = 3600;
	
	/** Snapshots enabled. */
	
	protected boolean snapshotsEnabled = true;
	
	/** The total worker threads. */
	protected int totalWorkerThreads = Runtime.getRuntime().availableProcessors() / 2;
	
	/** The total io threads. */
	protected int totalIOThreads = 1;
	
	/**
	 * Instantiates a new raw fs configuration.
	 */
	public RawFSConfiguration(){
		super();
	}

	/**
	 * Gets the rW buffer size.
	 *
	 * @return the rW buffer size
	 */
	public int getRWBufferSize() {
		return rwBufferSize;
	}

	/**
	 * Sets the rW buffer size.
	 *
	 * @param rwBufferSize the new rW buffer size
	 */
	public void setRWBufferSize(int rwBufferSize) {
		this.rwBufferSize = rwBufferSize;
	}

	/**
	 * Gets the snapshot interval.
	 *
	 * @return the snapshot interval
	 */
	public int getSnapshotInterval() {
		return snapshotInterval;
	}

	/**
	 * Sets the snapshot interval.
	 *
	 * @param snapshotInterval the new snapshot interval
	 */
	public void setSnapshotInterval(int snapshotInterval) {
		this.snapshotInterval = snapshotInterval;
	}

	/**
	 * Checks if is snapshots enabled.
	 *
	 * @return true, if is snapshots enabled
	 */
	public boolean isSnapshotsEnabled() {
		return snapshotsEnabled;
	}

	/**
	 * Sets the snapshots enabled.
	 *
	 * @param snapshotsEnabled the new snapshots enabled
	 */
	public void setSnapshotsEnabled(boolean snapshotsEnabled) {
		this.snapshotsEnabled = snapshotsEnabled;
	}

	/**
	 * Gets the total worker threads.
	 *
	 * @return the total worker threads
	 */
	public int getTotalWorkerThreads() {
		return totalWorkerThreads;
	}

	/**
	 * Sets the total worker threads.
	 *
	 * @param totalWorkerThreads the new total worker threads
	 */
	public void setTotalWorkerThreads(int totalWorkerThreads) {
		this.totalWorkerThreads = totalWorkerThreads;
	}

	/**
	 * Gets the total io threads.
	 *
	 * @return the total io threads
	 */
	public int getTotalIOThreads() {
		return totalIOThreads;
	}

	/**
	 * Sets the total io threads.
	 *
	 * @param totalIOThreads the new total io threads
	 */
	public void setTotalIOThreads(int totalIOThreads) {
		this.totalIOThreads = totalIOThreads;
	}
	
	
	
	
}
