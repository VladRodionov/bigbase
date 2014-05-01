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

// TODO: Auto-generated Javadoc
/**
 * The Interface Codec.
 */
public interface Codec {

	/** The Constant COMPRESSION_THRESHOLD. */
	public final static String COMPRESSION_THRESHOLD = "compression.threshold";
	
    /**
     * Compress the content in the given input buffer. After the compression,
     * you can retrieve the compressed data from the output buffer [pos() ... limit())
     * (compressed data size = limit() - pos() = remaining())
     * uncompressed - buffer[pos() ... limit()) containing the input data
     * compressed - output of the compressed data. Uses range [pos()..limit()].
     *
     * @param src the src
     * @param dst the dst
     * @return byte size of the compressed data.
     * @throws IOException Signals that an I/O exception has occurred.
     */
	public int compress(ByteBuffer src, ByteBuffer dst) throws IOException;
	
	/**
	 * Uncompress the content in the input buffer. The result is dumped
	 * to the specified output buffer. Note that if you pass the wrong data
	 * or the range [pos(), limit()) that cannot be uncompressed, your JVM might
	 * crash due to the access violation exception issued in the native code
	 * written in C++. To avoid this type of crash,
	 * use isValidCompressedBuffer(ByteBuffer) first.
	 *
	 * @param src - buffer[pos() ... limit()) containing the input data
	 * @param dst - output of the the uncompressed data. It uses buffer[pot()..]
	 * @return  uncompressed data size
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException;
	
	/**
	 * Gets the compression threshold.
	 *
	 * @return the compression threshold
	 */
	public int getCompressionThreshold();
	
	/**
	 * Sets the compression threshold.
	 *
	 * @param val the new compression threshold
	 */
	public void setCompressionThreshold(int val);
	
	/**
	 * Gets the type.
	 *
	 * @return the type
	 */
	public CodecType getType();
	
	/**
	 * Gets the avg compression ratio.
	 *
	 * @return the avg compression ratio
	 */
	public double getAvgCompressionRatio();
	
	public long getTotalProcessed();
	
	/**
	 * Sets the level.
	 *
	 * @param level the new level
	 */
	public void setLevel(int level);
	
	/**
	 * Gets the level.
	 *
	 * @return the level
	 */
	public int getLevel();
	
}
