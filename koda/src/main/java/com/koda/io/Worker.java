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
package com.koda.io;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

// TODO: Auto-generated Javadoc
/**
 * The Class Worker.
 *
 * @param <T> the generic type
 */
public abstract class Worker<T> extends Thread {

	/** The from queue. */
	protected BlockingQueue<T> fromQueue;
	
	/** The to queue. */
	protected BlockingQueue<T> toQueue;
	
	/** The finished. */
	protected boolean finished = false;
	
	/**
	 * Instantiates a new worker.
	 *
	 * @param name the name
	 * @param from the from
	 * @param to the to
	 */
	public Worker(String name, BlockingQueue<T> from, BlockingQueue<T> to)
	{
		super(name);
		this.fromQueue = from;
		this.toQueue = to;
	}
	

	/**
	 * Finish work.
	 */
	public void finishWork()
	{
		finished = true;
		// TODO check if we can interrupt
		// interrupt();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run()
	{
		boolean emptyQueue = false;
		while(!finished || !emptyQueue)
		{
			try {
				T data = null;						
				data = fromQueue.poll(100, TimeUnit.MILLISECONDS);								
				if(data == null){ 
					emptyQueue = fromQueue.size() == 0;					
					continue;
				}
				
				boolean last =process(data);
				toQueue.put(data);
				if(last) { break; }
				
			} catch (InterruptedException e) {
				if(finished && fromQueue.size() == 0){
					break;
				}
			} catch(IOException e)
			{
				//TODO
			}
			if(finished){
				emptyQueue = fromQueue.size() == 0;
				if(emptyQueue){ break; }				
			}
		}
		// Finish
		finish();
	}
	
	/**
	 * Process data.
	 *
	 * @param data the data
	 * @return true if last call
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	protected abstract boolean process(T data) throws IOException;
	
	/**
	 * Finish.
	 */
	protected abstract void finish();
	
	
}
