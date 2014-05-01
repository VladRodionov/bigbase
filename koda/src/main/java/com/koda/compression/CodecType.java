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

// TODO: Auto-generated Javadoc
/**
 * The Enum CodecType.
 */
public enum CodecType {

  /** No compression. */
  NONE(0),
  /** The SNAPPY. */
  SNAPPY(1),
  /** The DEFLATE (ZLIB). */
  DEFLATE(2),
  /** LZ4 */
  LZ4(3),
  /** LZ4-HC */
  LZ4HC(4),
  
  /** LZMA */
  LZMA(5);

  /** The id. */
  private int id;

  /**
   * Instantiates a new codec type.
   * 
   * @param id
   *          the id
   */
  private CodecType(int id) {
    this.id = id;
  }

  /**
   * Id.
   * 
   * @return the int
   */
  public int id() {
    return id;
  }

  /**
   * Gets the codec.
   * 
   * @return the codec
   */
  public Codec getCodec() {
    switch (id) {
      case 0:
        return null;
      case 1:
        return CodecFactory.getInstance().getCodec(CodecType.SNAPPY);
      case 2:
        return CodecFactory.getInstance().getCodec(CodecType.DEFLATE);
      case 3:
        return CodecFactory.getInstance().getCodec(CodecType.LZ4);
      case 4: 
        return CodecFactory.getInstance().getCodec(CodecType.LZ4HC);  
      case 5:
        return CodecFactory.getInstance().getCodec(CodecType.LZMA);
    }
    return null;
  }
  

  
}
