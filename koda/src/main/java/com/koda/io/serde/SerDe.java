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
package com.koda.io.serde;

//import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.koda.NativeMemory;
import com.koda.compression.Codec;
import com.koda.compression.CodecFactory;
import com.koda.io.serde.kryo.KryoSerializer;
import com.koda.io.serde.kryo.SerializerSerializer;
import com.koda.io.serde.kryo.WritableKryoSerializer;

// TODO: Auto-generated Javadoc
/**
 * The Class SerDe. Kryo -dependent, Compression - aware
 */
@SuppressWarnings("unchecked")
public class SerDe {
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger
	.getLogger(SerDe.class);
	
	/** The Constant SERDE_BUF_SIZE. */
	public final static String SERDE_BUF_SIZE="koda.serde.buffer.size";
	/** The Constant NULL_VALUE. */
	private final static int NULL_VALUE = -1;
	
	/** The Constant DEFAULT_SERDE. */
	private final static int DEFAULT_SERDE = -2;

/** TODO: Thread local maps. */
	/** The ser/de map. */
	private ConcurrentHashMap<Integer, SerDeMapEntry> mSerDeMap = new ConcurrentHashMap<Integer, SerDeMapEntry>();

	/** The serializers. */

  private ConcurrentHashMap<Class<?>, Serializer> mSerializers = new ConcurrentHashMap<Class<?>, Serializer>();

	/** The m serDe map thread local storage. */
	private final static ThreadLocal<HashMap<Integer,SerDeMapEntry>> sSerDeMapTLS = 
		new ThreadLocal<HashMap<Integer,SerDeMapEntry>>(){
		@Override protected HashMap<Integer, SerDeMapEntry> initialValue() {
            return new HashMap<Integer,SerDeMapEntry>();
		}
	};
	
	/** The m serializers tls. */
	private final static ThreadLocal<HashMap<Class<?>, Serializer>> sSerializersTLS = 
		new ThreadLocal<HashMap<Class<?>, Serializer>>(){
		@Override protected HashMap<Class<?>, Serializer> initialValue() {
            return new HashMap<Class<?>, Serializer>();
		}

	};
	
	/** The m temp buffer tls. */
	private final static ThreadLocal<ByteBuffer> sTempBufferTLS = new ThreadLocal<ByteBuffer>(){
		@Override protected ByteBuffer initialValue() {
            try {
				return NativeMemory.allocateDirectBuffer(64, 
						Integer.parseInt(System.getProperty(SERDE_BUF_SIZE, Integer.toString(4*1024*1024))));
			} catch (Exception e) {
				LOG.error("Can not init serde temp buffer", e);
			} 
			return null;
		}
	};
	
	/** The default serializer. */
	private Serializer mDefault;

	/** The m default registration optional. */
	private boolean mDefaultRegistrationOptional;

	/** The s instance. */
	private static SerDe sInstance;

	/**
	 * Gets the single instance of SerDe.
	 * 
	 * @return single instance of SerDe
	 */
	public static SerDe getInstance() {
		synchronized (SerDe.class) {
			if (sInstance == null) {
				sInstance = new SerDe();
			}
			return sInstance;
		}

	}

	
	/**
	 * Instantiates a new ser de.
	 */
	private SerDe()
	{
		registerDefaults();
	}
	
	
	/**
	 * Gets the temp buffer.
	 *
	 * @return the temp buffer
	 */
	private final ByteBuffer getTempBuffer()
	{
		ByteBuffer buf = SerDe.sTempBufferTLS.get();
		buf.clear();
		return buf;
	}
	
	/**
	 * Gets the serializer.
	 *
	 * @param cls the cls
	 * @return the serializer
	 */
	private final Serializer getSerializer(Class<?> cls)
	{
		HashMap<Class<?>, Serializer> localMap = SerDe.sSerializersTLS.get();
		Serializer ser = localMap.get(cls);
		
		if(ser != null) return ser;

		ser = mSerializers.get(cls);
		if(ser != null){
			localMap.put(cls, ser);
		}
		// Hack
		if(cls.getName().equals("java.nio.HeapByteBuffer")){
			return getSerializer(ByteBuffer.class);
		}
		return ser;
	}
	
	/**
	 * Gets the ser de map entry.
	 *
	 * @param classId the class id
	 * @return the ser de map entry
	 */
	private final SerDeMapEntry getSerDeMapEntry(int classId)
	{
		HashMap<Integer,SerDeMapEntry> localMap = SerDe.sSerDeMapTLS.get();
		SerDeMapEntry entry = localMap.get(classId);
		if(entry != null) return entry;
		entry = mSerDeMap.get(classId);
		if(entry != null){
			localMap.put(classId, entry);
		}
		return entry;
	}	
	/**
	 * Register default serializers.
	 */
	private  void registerDefaults() {
		setDefaultSerializer(new KryoSerializer());
	    setDefaultRegistrationOptional(true);
		
		registerSerializer(new ByteArraySerializer());
		//registerSerializer(new StringSerializer());	
		registerSerializer(new ByteBufferSerializer());

	}

	
	/**
	 * Gets the registered serializers.
	 *
	 * @return the registered serializers
	 */
	public Collection<Serializer> getRegisteredSerializers()
	{
		return mSerializers.values();
	}
	
	/**
	 * Register custom serializer.
	 * 
	 * @param serde
	 *            the serde
	 */
	public void registerSerializer(Serializer serde) {
		List<Class<?>> list = serde.getClassesAsList();
		Kryo kryo = ((KryoSerializer)mDefault).getKryo();
		SerializerSerializer ss = new SerializerSerializer(serde);
		
		for (Class<?> clz : list) {
			mSerializers.putIfAbsent(clz, serde);
			SerDeMapEntry tv = new SerDeMapEntry(serde.getClassID(clz), clz, serde);
			mSerDeMap.putIfAbsent(serde.getClassID(clz), tv);
			kryo.register(clz, ss);
		}

	}

	/**
	 * Register writable class.
	 *
	 * @param id the id
	 * @param w the w
	 */


	public void registerWritable(int id, Class<? extends Writable> w) {
		SerDeMapEntry tv = new SerDeMapEntry(id, w, null);
		mSerDeMap.putIfAbsent(id, tv);
		((KryoSerializer)mDefault).getKryo().register(w, WritableKryoSerializer.getInstance());
	}

	/**
	 * Register default.
	 *
	 * @param clz the clz
	 */
	public void registerDefault(Class<?> clz)
	{
		if(mDefault != null){
			((KryoSerializer)mDefault).getKryo().register(clz);
		}
	}
	
	
	/**
	 * Sets the default registration optional.
	 *
	 * @param v the new default registration optional
	 */
	private void setDefaultRegistrationOptional(boolean v)
	{
		if(mDefault != null){
			mDefaultRegistrationOptional = v;
			((KryoSerializer)mDefault).getKryo().setRegistrationOptional(v);
		}
	}
	
	/**
	 * Gets the default registration optional.
	 *
	 * @return the default registration optional
	 */
	public boolean getDefaultRegistrationOptional()
	{
		return mDefaultRegistrationOptional;
	}
	
	/**
	 * Unregister.
	 * 
	 * @param serde
	 *            the serde
	 */
	public void unregisterSerializer(Serializer serde) {
		List<Class<?>> list = serde.getClassesAsList();
		for (Class<?> clz : list) {
			mSerializers.remove(clz);
			mSerDeMap.remove(serde.getClassID(clz));
		}
	}

	/**
	 * Unregister writable.
	 *
	 * @param id the id
	 */

	public void unregisterWritable(int id) {
		mSerDeMap.remove(id);
	}

	/**
	 * Write (compression is disabled).
	 *
	 * @param buf the buf
	 * @param value the value
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
  public void write(ByteBuffer buf, Object value) throws IOException {
		if (value == null) {
			buf.putInt(NULL_VALUE);
			return;
		} else if (value instanceof Writable) {
			Writable w = (Writable) value;
			buf.putInt(w.getID());
			w.write(buf);
		} else { // Check do we have custom serde
			Class<?> clz = value.getClass();
			Serializer serde = getSerializer(clz);//mSerializers.get(clz);
			if (serde != null) {
				buf.putInt(serde.getClassID(clz));
				serde.write(buf, value);
			} else {
				// Use default
				if (mDefault != null) {
					buf.putInt(DEFAULT_SERDE);
					mDefault.write(buf, value);
				} else {
					throw new IOException("No serializers found for class: "
							+ clz.getName());
				}
			}
		}
//	  writeCompressed(buf, value, null);
	}

	 @SuppressWarnings("unused")
  private void writeInternal(ByteBuffer buf, Object value) throws IOException {
	    if (value == null) {
	      buf.putInt(NULL_VALUE);
	      return;
	    } else if (value instanceof Writable) {
	      Writable w = (Writable) value;
	      buf.putInt(w.getID());
	      w.write(buf);
	    } else { // Check do we have custom serde
	      Class<?> clz = value.getClass();
	      Serializer serde = getSerializer(clz);//mSerializers.get(clz);
	      if (serde != null) {
	        buf.putInt(serde.getClassID(clz));
	        serde.write(buf, value);
	      } else {
	        // Use default
	        if (mDefault != null) {
	          buf.putInt(DEFAULT_SERDE);
	          mDefault.write(buf, value);
	        } else {
	          throw new IOException("No serializers found for class: "
	              + clz.getName());
	        }
	      }
	    }
	  }
	
	/**
	 * Write with class.
	 *
	 * @param buf the buf
	 * @param value the value
	 * @param clz the clz
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writeWithClass(ByteBuffer buf, Object value, Class<?> clz) throws IOException {
		if (value == null) {
			buf.putInt(NULL_VALUE);
			return;
		} else if (value instanceof Writable) {
			Writable w = (Writable) value;
			//buf.putInt(w.getID());
			w.write(buf);
		} else { // Check do we have custom serde

			Serializer serde = getSerializer(clz);
			if (serde != null) {
				serde.write(buf, value);
			} else {
				// Use default
				if (mDefault != null) {
					mDefault.write(buf, value);
				} else {
					throw new IOException("No serializers found for class: "
							+ clz.getName());
				}
			}
		}
	}

	
	/**
	 * Write (compression is enabled if Codec != null).
	 * We use compression only for Values.
	 *
	 * @param buf the buf
	 * @param value the value
	 * @param codec the codec
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	public void writeCompressed(ByteBuffer buf, Object value, Codec codec) throws IOException {
		
		if(codec == null ){
		  // ADD support for dynamic codecs
		  // write 0 - say we do not use compression
		  //buf.putInt(0);
		  // put 1 more byte
		  buf.put((byte)0);
		  write(buf, value); 
		  return;
		}
		
		ByteBuffer temp = getTempBuffer();
		int ct = codec.getCompressionThreshold();
		
		// temp position is 0
		write(temp, value);
		int pos = temp.position();    
		temp.flip();
		if(pos < ct){
			//buf.putInt(0); // NO-COMPRESSION (short value)
			buf.put((byte)0);
			buf.put(temp);
		} else{
			int bpos = buf.position();
			// advance buf on 5 bytes
			buf.position(bpos+5);
			int size = codec.compress(temp, buf);
			// write size of compressed data
			buf.put(bpos, (byte) codec.getType().id());
			buf.putInt(bpos+1, size);
			// write codec type
			
			//TODO this is Snappy codec thing
			buf.position(buf.limit());


		}
	}
	
	/**
	 * Write compressed with class.
	 *
	 * @param buf the buf
	 * @param value the value
	 * @param codec the codec
	 * @param clz the clz
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void writeCompressedWithClass(ByteBuffer buf, Object value, Codec codec, Class<?> clz) throws IOException {
		
		if(codec == null ){
		  //buf.putInt(0);
		  buf.put((byte) 0);
			writeWithClass(buf, value, clz); 
			return;
		}
		
		ByteBuffer temp = getTempBuffer();
		int ct = codec.getCompressionThreshold();
		
		// temp position is 0
		writeWithClass(temp, value, clz);
		int pos = temp.position();
		temp.flip();
		if(pos < ct){
			//buf.putInt(0); // NO-COMPRESSION (short value)
			buf.put((byte) 0);
			buf.put(temp);
		} else{
			int bpos = buf.position();
			// advance buf on 5 bytes
			buf.position(bpos+5);
			int size = codec.compress(temp, buf);
			// write size of compresed data
			buf.put(bpos , (byte) codec.getType().id());
			buf.putInt(bpos+1, size);
			
			//TODO this is Snappy codec thing
			buf.position(buf.limit());


		}
	}
	/**
	 * Read value from buffer (compression is disabled).
	 *
	 * @param buf the buf
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object read(ByteBuffer buf) throws IOException {
		int classID = buf.getInt();
		if (classID == NULL_VALUE)
			return null;
		if (classID == DEFAULT_SERDE) {
			return mDefault.read(buf);
		}
		SerDeMapEntry tv = getSerDeMapEntry(classID);//mSerDeMap.get(classID);
		if (tv == null) {
			throw new IOException("No serializer found for classID=" + classID);
		} else {
			Class<?> clz = tv.getClassValue();
			Serializer serde = tv.getSerializerValue();
			if (serde != null) {
				return serde.read(buf);
			}

			Object value = null;
			try {
				value = clz.newInstance();
			} catch (Exception e) {
				throw new IOException(e);
			}

			if (value instanceof Writable) {
				Writable w = (Writable) value;
				w.read(buf);
				return w;
			} else {
				throw new IOException("No serde for class: " + clz.getName());
			}

		}
	   
	}

	 @SuppressWarnings("unused")
  private Object readInternal(ByteBuffer buf) throws IOException {
	    int classID = buf.getInt();
	    if (classID == NULL_VALUE)
	      return null;
	    if (classID == DEFAULT_SERDE) {
	      return mDefault.read(buf);
	    }
	    SerDeMapEntry tv = getSerDeMapEntry(classID);//mSerDeMap.get(classID);
	    if (tv == null) {
	      throw new IOException("No serializer found for classID=" + classID);
	    } else {
	      Class<?> clz = tv.getClassValue();
	      Serializer serde = tv.getSerializerValue();
	      if (serde != null) {
	        return serde.read(buf);
	      }

	      Object value = null;
	      try {
	        value = clz.newInstance();
	      } catch (Exception e) {
	        throw new IOException(e);
	      }

	      if (value instanceof Writable) {
	        Writable w = (Writable) value;
	        w.read(buf);
	        return w;
	      } else {
	        throw new IOException("No serde for class: " + clz.getName());
	      }

	    }

	  }
	/**
	 * Read with value.
	 *
	 * @param buf the buf
	 * @param value the value
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object readWithValue(ByteBuffer buf, Object value) throws IOException {
		int classID = buf.getInt();
		if (classID == NULL_VALUE)
			return null;
		if (classID == DEFAULT_SERDE) {
			return mDefault.read(buf, value);
		}
		SerDeMapEntry tv = getSerDeMapEntry(classID);
		if (tv == null) {
			throw new IOException("No serializer found for classID=" + classID);
		} else {
			Class<?> clz = tv.getClassValue();
			Serializer serde = tv.getSerializerValue();
			if (serde != null) {
				return serde.read(buf, value);
			}

			try {
				if(value == null )value = clz.newInstance();
			} catch (Exception e) {
				throw new IOException(e);
			}

			if (value instanceof Writable) {
				Writable w = (Writable) value;
				w.read(buf);
				return w;
			} else {
				throw new IOException("No serde for class: " + clz.getName());
			}

		}

	}
	
	/**
	 * Read value from buffer (compression is disabled).
	 *
	 * @param buf the buf
	 * @param clz the clz
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object readWithClass(ByteBuffer buf, Class<?> clz) throws IOException {


			Serializer serde = getSerializer(clz);
			if (serde != null) {
				return serde.read(buf);
			}

			Object value = null;
			try {
				value = clz.newInstance();
			} catch (Exception e) {
				throw new IOException(e);
			}

			if (value instanceof Writable) {
				Writable w = (Writable) value;
				w.read(buf);
				return w;
			} else {
				return mDefault.read(buf);
			}

		

	}
	
	/**
	 * Read value from buffer (compression is enabled id codec != null).
	 *
	 * @param buf the buf
	 * @param codec the codec
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	
	 public Object readCompressed(ByteBuffer buf) throws IOException {
	    // FIXME: 	    
	    //int compLen = buf.getInt();
	    // skip 1
	    int codecId = buf.get();

	    if(codecId > 0){
	    	int compLen = buf.getInt();
	    	Codec codec = CodecFactory.getCodec(codecId);       
	    	ByteBuffer temp = getTempBuffer();
	    	// TODO: size of compressed data in limit()
	    	buf.limit(buf.position() + compLen);      
	    	@SuppressWarnings("unused")
            int size = codec.decompress(buf, temp);
	        return read(temp);
	    } else if(codecId == 0){
	        return read(buf);
	    } else{
	        throw new IOException("Corrupted stream: unknown codec ="+codecId);
	    }

	  }
	

	
	 public Object readCompressedWithClass(ByteBuffer buf, Class<?> clz) throws IOException {
	    	    
		// int compLen = buf.getInt();
	    int codecId = buf.get();
	    if(codecId > 0){
	    	int compLen = buf.getInt();
            Codec codec = CodecFactory.getCodec(codecId);       

            ByteBuffer temp = getTempBuffer();
            // TODO: size of compressed data in limit()
            buf.limit(buf.position() + compLen);
	      
            codec.decompress(buf, temp);
            temp.position(0);
            return readWithClass(temp, clz);
	    } else if(codecId == 0){
	      return readWithClass(buf, clz);
	    } else{
	      throw new IOException("Corrupted stream: unknown codec ="+codecId);
	    }

	  }
	/**
	 * Read.
	 *
	 * @param buf the buf
	 * @param codec the codec
	 * @param value the value
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	
	 public Object readCompressedWithValue(ByteBuffer buf,  Object value) throws IOException {

	    
	    //int compLen = buf.getInt();
	    int codecId = buf.get();
	    
	    if(codecId > 0){
		    int compLen = buf.getInt();

	      Codec codec = CodecFactory.getCodec(codecId);	      
	      ByteBuffer temp = getTempBuffer();
	      // TODO: size of compressed data in limit()
	      buf.limit(buf.position() + compLen);
	      codec.decompress(buf, temp);
	      temp.position(0);
	      return readWithValue(temp, value);
	    } else if(codecId == 0){// No compression
	      return readWithValue(buf, value);
	    } else{
	      throw new IOException("Corrupted stream: unknown codec ="+codecId);
	    }

	  }
	  
	
	/**
	 * Read.
	 *
	 * @param buf the buf
	 * @param codec the codec
	 * @param value the value
	 * @param clz the clz
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */

	
	 public Object readCompressedWithValueAndClass(ByteBuffer buf,  Object value, Class<?> clz) throws IOException {

	    
	    //int compLen = buf.getInt();
	    int codecId = buf.get();
	    if(codecId > 0){
		    int compLen = buf.getInt();

	      Codec codec = CodecFactory.getCodec(codecId);       

	      ByteBuffer temp = getTempBuffer();
	      // TODO: size of compressed data in limit()
	      buf.limit(buf.position() + compLen);
	      codec.decompress(buf, temp);
	      temp.position(0);
	      return readWithValueAndClass(temp, value, clz);
	    } else if(codecId == 0){// No compression
	      return readWithValueAndClass(buf, value, clz);
	    } else{
	      throw new IOException("Corrupted stream: unknown codec ="+codecId);
	    }

	  }
	
	/**
	 * Read value using into existing value.
	 *
	 * @param buf the buf
	 * @param val the val
	 * @param clz the clz
	 * @return the object
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public Object readWithValueAndClass(ByteBuffer buf, Object val, Class<?> clz) throws IOException {

			Serializer serde = getSerializer(clz);
			if (serde != null) {
				serde.read(buf, val);
				return val;
			}

			if (val instanceof Writable) {
				Writable w = (Writable) val;
				w.read(buf);
				return w;
			} else {
				return mDefault.read(buf, val);
			}

	}	
	
	/**
	 * Gets the default serializer.
	 * 
	 * @return the default serializer
	 */
	public Serializer getDefaultSerializer() {
		return mDefault;
	}

	/**
	 * Sets the default serializer.
	 * 
	 * @param def
	 *            the new default serializer
	 */
	public void setDefaultSerializer(Serializer def) {
		this.mDefault = def;
	}
}
