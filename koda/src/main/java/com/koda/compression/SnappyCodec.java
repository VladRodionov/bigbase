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
package com.koda.compression;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.xerial.snappy.Snappy;

// TODO: Auto-generated Javadoc
/**
 * The Class SnappyCodec.
 */
public class SnappyCodec implements Codec {

	/** The min comp size. */
	private int minCompSize;
	
	/** The total size. */
	long totalSize = 0;
	
	/** The total comp size. */
	long totalCompSize = 0;
	
	/** The level. */
	int level =1;
	/**
	 * Instantiates a new snappy codec.
	 */
	SnappyCodec(){
		minCompSize = Integer.parseInt(System.getProperty(COMPRESSION_THRESHOLD));
	}
	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#compress(java.nio.ByteBuffer, java.nio.ByteBuffer)
	 */
	@Override
	public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {
		this.totalSize += (src.limit() - src.position());
		int compSize = Snappy.compress(src, dst);
		this.totalCompSize += compSize;
		return compSize;
	}

	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#decompress(java.nio.ByteBuffer, java.nio.ByteBuffer)
	 */
	@Override
	public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {		
		return Snappy.uncompress(src, dst);
	}


	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#getCompressionThreshold()
	 */
	@Override
	public int getCompressionThreshold() {

		return minCompSize;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#setCompressionThreshold(int)
	 */
	@Override
	public void setCompressionThreshold(int val) {
		
		minCompSize = val;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#getType()
	 */
	@Override
	public CodecType getType() {
		
		return CodecType.SNAPPY;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#getAvgCompressionRatio()
	 */
	@Override
	/**
	 * TODO
	 * This is not very accurate: some small entries < threshold are not compressed
	 */
	public double getAvgCompressionRatio() {
		if(totalCompSize == 0){
			return 1.d;
		} else{
			return ((double)totalSize)/totalCompSize;
		}
	}
	
	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#getLevel()
	 */
	@Override
	public int getLevel() {

		return level;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#setLevel(int)
	 */
	@Override
	public void setLevel(int level) {
		this.level = level;
		
	}
  @Override
  public long getTotalProcessed() {
    return totalSize;
  }
}
