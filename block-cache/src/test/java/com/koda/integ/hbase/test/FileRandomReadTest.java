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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;

// TODO: Auto-generated Javadoc
/**
 * The Class StorageFileScanner.
 */
public class FileRandomReadTest {
	
	static int numThreads = 1;
	// 32K
	static int recordSize = 32*1024;
	// 3.2 GB
	static long fileSize = 2000000L * recordSize;
	// we will do reverse scan of a file
	static String path = "/cache";
	
	static String NUM_THREADS ="-t";
	static String RECORD_SIZE = "-r";
	static String FILE_SIZE = "-s";
	static String FILE_PATH = "-f";
	
	static File testFile;
	static RandomAccessFile rafAccess;
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IOException {		
		// Parse arguments
		parseArgs(args);
		dumpArgs();
		createNewFile();
		
	}
	
	private static void createNewFile() throws IOException {
		File dir = new File(path);
		testFile = File.createTempFile("test", "data", dir);
		testFile.deleteOnExit();

		rafAccess = new RandomAccessFile(testFile, "rw");
		//rafAccess.setLength(fileSize);
		
		System.out.println("File length set to: " + rafAccess.length());
		
		ByteBuffer buf = ByteBuffer.allocateDirect(recordSize * 100);
		FileChannel fc = rafAccess.getChannel();
		fc.position(0);
		int hundredMB = 0;
		while(fc.position() < fileSize){
			int written = 0;
			while( (written +=fc.write(buf)) < buf.capacity());
			buf.flip();
		
			int hmb = (int ) (fc.position() / (100* 1024 *1024));
			if (hmb > hundredMB){
				hundredMB = hmb;
				System.out.println("Written "+((long)hundredMB) * (100 * 1024 *1024));
			}
			fc.force(true);
			
						
		}
		fc.force(true);
		
		readFileRandom();
		
	}

	private static void dumpArgs() {
		System.out.println("Threads   ="+numThreads);
		System.out.println("Record    ="+recordSize);
		System.out.println("Path      ="+path);
		System.out.println("File size ="+fileSize);
		
	}

	private static void parseArgs(String[] args) {

		int i = 0;
		while (i < args.length) {

			if (args[i].equals(NUM_THREADS)) {
				numThreads = Integer.parseInt(args[++i]);
			} else if(args[i].equals(RECORD_SIZE)){
				recordSize = Integer.parseInt(args[++i]);
			} else if(args[i].equals(FILE_SIZE)){
				fileSize = Long.parseLong(args[++i]);
			} else if(args[i].equals(FILE_PATH)){
				path = args[++i];
			}				
			 
		}
	}
	
	/**
	 * Reads 
	 *
	 * @param args the args
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private static void readFileRandom() throws IOException{
		
		
		RandomAccessFile file = rafAccess;
		
		System.out.println("File: "+testFile.getName()+" test starts. File Size="+file.length());
		FileChannel fc = file.getChannel();
		ByteBuffer buf = ByteBuffer.allocateDirect(recordSize);

		long usableSize = fileSize / 2;
		
		int slots = (int) (usableSize / recordSize);
		Random r = new Random();
		
		long pos = fileSize ;
		int recordNumber = 0;
        int maxReads = 1000;
		long startTime = System.currentTimeMillis();
		try{
			while( recordNumber < maxReads ){
				int toRead = r.nextInt(slots);
				pos = ((long)toRead) * recordSize;
				fc.position(pos);
				int total = 0;
				buf.clear();
				while((total += fc.read(buf)) < recordSize);
				
				total = 0;
				recordNumber++;
				if(recordNumber % 100 == 0){
					System.out.println(recordNumber);
				}
			}
		} finally{
			fc.close();
			file.close();
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Random Read total blocks ="+recordNumber+ " of size "+recordSize+" in "+
				(endTime-startTime+"ms"));
		
	}


}
