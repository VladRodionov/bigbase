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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.koda.io.serde.SerDe;

// TODO: Auto-generated Javadoc
/**
 * The Class DeflateCodec.
 */
public class DeflateCodec implements Codec {

	/** The level. */
	private int level = 6; 
	
	/** The min comp size. */
	private int minCompSize = 100;
		
	/** The total size. */
	private long totalSize = 0;
	
	/** The total comp size. */
	private long totalCompSize = 0;	
	
	/** The deflater tls. */
	private ThreadLocal<Deflater> deflaterTLS = new ThreadLocal<Deflater>();
	
	/** The inflater tls. */
	private ThreadLocal<Inflater> inflaterTLS = new ThreadLocal<Inflater>();
	
	/** The in tls. */
	private ThreadLocal<byte[]> inTLS = new ThreadLocal<byte[]>(){

		@Override
		protected byte[] initialValue() {
			int size = Integer.parseInt(System.getProperty(SerDe.SERDE_BUF_SIZE));
			return new byte[size];
		}
		
	};
	
	/** The out tls. */
	private ThreadLocal<byte[]> outTLS = new ThreadLocal<byte[]>(){

		@Override
		protected byte[] initialValue() {
			int size = Integer.parseInt(System.getProperty(SerDe.SERDE_BUF_SIZE));
			return new byte[size];
		}
		
	};
	
	/**
	 * Instantiates a new deflate codec.
	 */
	public DeflateCodec() {}		
	
	
	/**
	 * Gets the deflater.
	 *
	 * @return the deflater
	 */
	private Deflater getDeflater()
	{
		Deflater d = deflaterTLS.get();
		if(d == null){
			d = new Deflater(level);
			deflaterTLS.set(d);
		}
		return d;
	}
	
	
	/**
	 * Gets the inflater.
	 *
	 * @return the inflater
	 */
	private Inflater getInflater()
	{
		Inflater in = inflaterTLS.get();
		if(in == null)
		{
			in = new Inflater();
			inflaterTLS.set(in);
		}
		return in;
	}
	
	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#compress(java.nio.ByteBuffer, java.nio.ByteBuffer)
	 */
	@Override
	public int compress(ByteBuffer src, ByteBuffer dst) throws IOException {
		int size = src.limit() - src.position();
		this.totalSize += size;
		byte[] in = inTLS.get();
		byte[] out = outTLS.get();
		src.get(in, 0, size);
		Deflater d = getDeflater();
		d.reset();
		d.setInput(in, 0, size);
		d.finish();
		int compSize = d.deflate(out);
		this.totalCompSize += compSize;
		dst.put(out, 0, compSize);
		dst.limit(dst.position());
		return compSize;
	}

	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#decompress(java.nio.ByteBuffer, java.nio.ByteBuffer)
	 */
	@Override
	public int decompress(ByteBuffer src, ByteBuffer dst) throws IOException {
		int size = src.limit() - src.position();
		
		byte[] in = inTLS.get();
		byte[] out = outTLS.get();
		src.get(in, 0, size);
		Inflater inf = getInflater();
		inf.reset();
		inf.setInput(in, 0, size);
		int decompSize =0;
		try {
			decompSize = inf.inflate(out);
		} catch (DataFormatException e) {
			throw new IOException(e);
		}
    int dpos = dst.position();
		dst.put(out, 0, decompSize);
		dst.position(dpos);
		dst.limit(dpos + decompSize);
		return decompSize;
	}

	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#getAvgCompressionRatio()
	 */
	@Override
	public double getAvgCompressionRatio() {
		if(totalCompSize == 0){
			return 1.d;
		} else{
			return ((double)totalSize)/totalCompSize;
		}
	}

	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#getCompressionThreshold()
	 */
	@Override
	public int getCompressionThreshold() {
		return minCompSize;
	}

	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#getType()
	 */
	@Override
	public CodecType getType() {
		
		return CodecType.DEFLATE;
	}

	/* (non-Javadoc)
	 * @see com.koda.compression.Codec#setCompressionThreshold(int)
	 */
	@Override
	public void setCompressionThreshold(int val) {
		this.minCompSize = val;

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
