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

import org.apache.log4j.Logger;

public class LZMACodec implements Codec {

  /** The Constant LOG. */
  @SuppressWarnings("unused")
    private final static Logger LOG = Logger.getLogger(LZMACodec.class);
  
  /** The min comp size. */
  private int minCompSize = 100;
  
  /** The total size. */
  private long totalSize = 0;
  
  /** The total comp size. */
  private long totalCompSize = 0;
  
  /** The level. */
  private int level = 1;
  /**
   * This codec was disabled since the benefits are questionable but 
   * performance is very poor.
   * 
   */
  public LZMACodec(){
    minCompSize = Integer.parseInt(System.getProperty(COMPRESSION_THRESHOLD, "100"));
  }
  
  @Override
  public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {
    // testing with defaults
//    totalSize += src.remaining();
//    int dstPos = dst.position();
//    
//    InputStream is = new BufferedInputStream(Utils.newInputStream(src));
//    OutputStream out = new BufferedOutputStream(Utils.newOutputStream(dst));
//    SevenZip.Compression.LZMA.Encoder encoder = new SevenZip.Compression.LZMA.Encoder();
//    encoder.SetAlgorithm(2);
//    encoder.SetDictionarySize(1 << 15);
//    encoder.SetNumFastBytes(128);
//    encoder.SetMatchFinder(1);   
//    encoder.SetLcLpPb(3, 0, 2);    
//    encoder.SetEndMarkerMode(false);
//    encoder.WriteCoderProperties(out);
//    
//    long dataSize = src.remaining();
//
//    for (int i = 0; i < 8; i++)
//      out.write((int)(dataSize >>> (8 * i)) & 0xFF);
//    encoder.Code(is, out, dataSize, -1, null);
//    out.close();    
//    int compressedSize = dst.position() - dstPos;
//    totalCompSize += compressedSize;
//    
//    int newLimit = dst.position();
//    dst.position(dstPos);
//    dst.limit(newLimit);
//    return compressedSize;
    return 0;
  }

  @Override
  public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
//    int dstPos = dst.position();
//    int propertiesSize = 5;
//    OutputStream outStream = new BufferedOutputStream(Utils.newOutputStream(dst));
//    InputStream inStream   = new BufferedInputStream(Utils.newInputStream(src));
//    
//    byte[] properties = new byte[propertiesSize];
//    if (inStream.read(properties, 0, propertiesSize) != propertiesSize)
//      throw new IOException("input corrupted");
//    SevenZip.Compression.LZMA.Decoder decoder = new SevenZip.Compression.LZMA.Decoder();
//    if (!decoder.SetDecoderProperties(properties))
//      throw new IOException("Incorrect stream properties");
//    long outSize = 0;
//    for (int i = 0; i < 8; i++)
//    {
//      int v = inStream.read();
//      if (v < 0)
//        throw new IOException("Can't read stream size");
//      outSize |= ((long)v) << (8 * i);
//    }
//    
//    if (!decoder.Code(inStream, outStream, outSize))
//      throw new IOException("Error in data stream");
//  
//    
//    outStream.close();    
//    
//    int decompressedSize = dst.position() - dstPos;
//    int newLimit = dst.position();
//    dst.position(dstPos);    
//    dst.limit(newLimit);
//    
//    return decompressedSize;
    return 0;
  }

  @Override
  public double getAvgCompressionRatio() {
    if(totalCompSize == 0){
      return 1.d;
    } else{
      return ((double)totalSize)/totalCompSize;
    }
  }

  @Override
  public int getCompressionThreshold() {
    return minCompSize;
  }

  @Override
  public int getLevel() {
    return this.level;
  }

  @Override
  public CodecType getType() {
    return CodecType.LZMA;
  }

  @Override
  public void setCompressionThreshold(int val) {
    this.minCompSize = val;
  }

  @Override
  public void setLevel(int level) {
    this.level = level;
  }

  @Override
  public long getTotalProcessed() {
    return totalSize;
  }
  
  public static void main(String[] args) throws IOException{
    
    String str = 
      "6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666"+
      "6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666"+
      "6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666"+
      "6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666"+
      "6666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666";
    
   str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
//    str += str;
    ByteBuffer src = ByteBuffer.allocateDirect(10240000);
    ByteBuffer dst = ByteBuffer.allocateDirect(10240000);
    Codec codec = new LZMACodec();
    
    byte[] buf = str.getBytes();
    src.put(buf);
    src.flip();
    int compSize = codec.compress(src, dst);
    System.out.println("Size="+ str.length() +" compressed ="+compSize);
    
    src.clear();
    
    int decSize = codec.decompress(dst, src);
    System.out.println("Size="+ str.length() +" decompressed ="+decSize);        
    
  }
}
