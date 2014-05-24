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

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;

/**
 * A factory for creating Codec objects.
 */
public class CodecFactory {

    private static Logger logger = Logger.getLogger(CodecFactory.class);

            /** The s instance. */
    private static CodecFactory sInstance;

    /**
     * Boolean array with TRUE value for each codec if supported, FALSE otherwise
     */
    private static boolean[] supportedCodecs = new boolean[CodecType.values().length];
    private static Codec[] codecs = {
        new SnappyCodec(),
        new DeflateCodec(),
        new LZ4Codec(),
        new LZ4HCCodec()/*,
        new LZMACodec()
        */
    };;
	
	/**
	 * Gets the single instance of CodecFactory.
	 *
	 * @return single instance of CodecFactory
	 */
	public synchronized static CodecFactory getInstance()
	{
		if(sInstance == null){
			sInstance = new CodecFactory();
		}
		return sInstance;
	}

    private CodecFactory() {
        // check codec availability

        // test buffer to compress
        ByteBuffer buf = ByteBuffer.allocateDirect(256);
        for (int i = 0; i < 256; i++) {
            buf.put( (byte) i );
        }

        // test each codec and memorize if it is supported
        for (Codec codec : codecs) {
            supportedCodecs[codec.getType().ordinal()] = checkCodec(codec, buf);
        }
    }

    private boolean checkCodec(Codec codec, ByteBuffer buf) {
        try {
            buf.rewind();
            ByteBuffer buf2 = ByteBuffer.allocateDirect(512);
            @SuppressWarnings("unused")
            int compressedSize = codec.compress(buf, buf2);
            return true;
        } catch (Throwable ex) {
            logger.warn("Codec " + codec.getType().toString() + " is not supported", ex);
            return false;
        }
    }
	
	/**
	 * Gets the codec.
	 *
	 * @param type the type
	 * @return the codec
	 */
	public Codec getCodec(CodecType type)
	{
        if (!supportedCodecs[type.ordinal()]) {
            return null;
        }
		switch (type){
			case SNAPPY: return new SnappyCodec();
			case DEFLATE: return new DeflateCodec();
			case LZ4: return new LZ4Codec();
			case LZ4HC: return new LZ4HCCodec();
			//case LZMA: return new LZMACodec();
			case NONE: return null;
		}
		return null;
	}
	 /**
   * Gets the codec.
   *
   * @param type the type
   * @return the codec
   */
  public static Codec getCodec(int id)
  {

    switch (id){
      case 1: return codecs[0];
      case 2: return codecs[1];
      case 3: return codecs[2];
      case 4: return codecs[3];
      // No codec
      case 0: return null;
    }
    return null;
  }
  
  public static Codec[] getCodecs()
  {
    return codecs;
  }
}
