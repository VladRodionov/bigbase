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
public class StorageFileScanner {
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(StorageFileScanner.class);
	
	
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

		
		RandomAccessFile file = new RandomAccessFile(fileName,"r");
		LOG.info("File: "+fileName+" verification starts. File Size="+file.length());
		FileChannel fc = file.getChannel();
		
		MappedByteBuffer buffer = fc.map(MapMode.READ_ONLY, 0, file.length());
		int recordNumber = 0;

		try{
			while(buffer.hasRemaining()){
				LOG.info(buffer.position());
				int size = buffer.getInt();
				recordNumber++;
				// skip 'size '
				int pos = buffer.position();
				buffer.position(pos + size );
			
			}
		} finally{
			fc.close();
			file.close();
		}
		LOG.info("File: "+fileName+" - OK. Total blocks ="+recordNumber);
		
	}


}
