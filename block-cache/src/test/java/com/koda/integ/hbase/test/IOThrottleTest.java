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
package com.koda.integ.hbase.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.log4j.Logger;

// TODO: Auto-generated Javadoc
/**
 * The Class StorageFileScanner.
 */
public class IOThrottleTest {
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(IOThrottleTest.class);
	
	/** The max read througput. */
	private static int maxReadThrougput;
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IOException {
		
		verifyFile(args);
	}
	
	/**
	 * Verify file.
	 *
	 * @param args the args
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void verifyFile(String[] args) throws IOException{
		
		String fileName = args[0];
		maxReadThrougput = Integer.parseInt(args[1]);

		
		RandomAccessFile file = new RandomAccessFile(fileName,"r");
		LOG.info("File: "+fileName+" verification starts. File Size="+file.length());
		FileChannel fc = file.getChannel();
		
		MappedByteBuffer buffer = fc.map(MapMode.READ_ONLY, 0, file.length());
		long fileLength = file.length();
		int recordNumber = 0;
		long startTime = System.currentTimeMillis();
		try{
			while(buffer.hasRemaining()){
				//LOG.info(buffer.position());
				int size = buffer.getInt();
				recordNumber++;
				// skip 'size '
				int pos = buffer.position();
				buffer.position(pos + size );
				
				ioThrottle(startTime, pos);
			}
		} finally{
			fc.close();
			file.close();
		}
		LOG.info("File: "+fileName+" - OK. Total blocks ="+recordNumber+
				" expected time="+(fileLength * 1000/(maxReadThrougput * 1000*1000)) 
				+" actual="+(System.currentTimeMillis() - startTime));
		
	}
	
	/**
	 * Io throttle.
	 *
	 * @param startTime the start time
	 * @param oldOffset the old offset
	 */
	private static void ioThrottle(long startTime, int oldOffset) {
		
		long time = System.currentTimeMillis();
		//if(time - startTime < 10) return; // do not do any throttling first 10ms
		long expectedSize = (long)(((double)(time - startTime)/ 1000) * maxReadThrougput * 1000000);
		if(oldOffset > expectedSize){
			long sleep = (oldOffset - expectedSize) / (maxReadThrougput * 1000);
			try {
				//LOG.info("Sleep "+sleep+"ms");
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
			}
		}
		
	}

}
