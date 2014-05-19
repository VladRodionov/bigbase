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

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

// TODO: Auto-generated Javadoc
/**
 * The Class BlockCacheKey.
 */
public class BlockCacheKeyTest implements  java.io.Serializable {
  
  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 1L;

  /** The hfile name. */
  private final String hfileName;
  
  /** The offset. */
  private final long offset;
  
  /** The encoding. */
  private final DataBlockEncoding encoding;

  /**
   * Instantiates a new block cache key.
   *
   * @param file the file
   * @param offset the offset
   * @param encoding the encoding
   * @param blockType the block type
   */
  public BlockCacheKeyTest(String file, long offset, DataBlockEncoding encoding,
      BlockType blockType) {
    this.hfileName = file;
    this.offset = offset;
    // We add encoding to the cache key only for data blocks. If the block type
    // is unknown (this should never be the case in production), we just use
    // the provided encoding, because it might be a data block.
    this.encoding = (blockType == null || blockType.isData()) ? encoding :
        DataBlockEncoding.NONE;
  }

  /**
   * Construct a new BlockCacheKey.
   *
   * @param file The name of the HFile this block belongs to.
   * @param offset Offset of the block into the file
   */
  public BlockCacheKeyTest(String file, long offset) {
    this(file, offset, DataBlockEncoding.NONE, null);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return hfileName.hashCode() * 127 + (int) (offset ^ (offset >>> 32)) +
        encoding.ordinal() * 17;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (o instanceof BlockCacheKeyTest) {
      BlockCacheKeyTest k = (BlockCacheKeyTest) o;
      return offset == k.offset
          && (hfileName == null ? k.hfileName == null : hfileName
              .equals(k.hfileName));
    } else {
      return false;
    }
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return hfileName + "_" + offset
        + (encoding == DataBlockEncoding.NONE ? "" : "_" + encoding);
  }

  /**
   * Strings have two bytes per character due to default Java Unicode encoding
   * (hence length times 2).
   *
   * @return the long
   */

  public long heapSize() {
    return ClassSize.align(ClassSize.OBJECT + 2 * hfileName.length() +
        Bytes.SIZEOF_LONG + 2 * ClassSize.REFERENCE);
  }

  // can't avoid this unfortunately
  /**
   * Gets the hfile name.
   *
   * @return The hfileName portion of this cache key
   */
  public String getHfileName() {
    return hfileName;
  }

  /**
   * Gets the data block encoding.
   *
   * @return the data block encoding
   */
  public DataBlockEncoding getDataBlockEncoding() {
    return encoding;
  }
}
