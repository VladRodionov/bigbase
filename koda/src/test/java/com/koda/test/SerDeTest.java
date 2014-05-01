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
package com.koda.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.koda.NativeMemory;
import com.koda.NativeMemoryException;
import com.koda.io.serde.SerDe;

// TODO: Auto-generated Javadoc
/**
 * The Class SerDeTest.
 */
public class SerDeTest {

	/** The N. */
	private static int N = 1000000;
	
	/** The Constant LOG. */
	private final static Logger LOG = Logger.getLogger(SerDeTest.class);
	
	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws NativeMemoryException the native memory exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws NativeMemoryException, IOException
	{
		SerDe manager = SerDe.getInstance();
		ByteBuffer buf = NativeMemory.allocateDirectBuffer(64, 4096);
		
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		
		String s = "123456789012345678901234567890";
		byte[] array = new byte[1000];
		
		//LOG.info("hash="+array.hashCode());
		
		
		for(int i = 0; i < array.length; i++)
		{
			array[i] = (byte)i;
		}
		
		//LOG.info("hash="+array.hashCode());
				
		
		
		long t1 = System.currentTimeMillis();
		
		for(int i=0; i < N ; i++){
			manager.write(buf, array);
			buf.flip();
			@SuppressWarnings("unused")
            byte[] arr = (byte[])manager.readWithValue(buf, array);
			
			buf.clear();
		}
		long t2 = System.currentTimeMillis();
		
		LOG.info("Time to serde 1K array "+N+" times="+(t2-t1)+" ms");

		buffer.put(s.getBytes());
		//buffer.flip();
		t1 = System.currentTimeMillis();
		
		for(int i=0; i < N; i++)
		{
			buffer.flip();
			manager.write(buf, buffer);
			buf.flip();
			ByteBuffer bb = (ByteBuffer)manager.read(buf);
			if(i == N-1){
				LOG.info(bb.position()+" "+bb.limit()+" s ="+s.length());
			}
			buf.clear();
			//LOG.info(bb.position()+" "+bb.limit()+" s ="+s.length());
		}
		t2 = System.currentTimeMillis();
		
		LOG.info("Time to serde/deserde "+s.length()+" ByteBuffer "+N+" times="+(t2-t1)+" ms");
		
		t1 = System.currentTimeMillis();
		int len = 0;
		for(int i=0; i < N ; i++){
			manager.write(buf, s);
			len = buf.position();
			buf.flip();
			@SuppressWarnings("unused")
            String ss = (String) manager.read(buf);
			buf.clear();
		}
		t2 = System.currentTimeMillis();

		LOG.info("Time to serde string "+N+" times="+(t2-t1)+" ms. Binary size of "+s+" ="+len);
		
		manager.registerDefault(ByteWrapper.class);
		
		t1 = System.currentTimeMillis();
		len = 0;
		for(int i=0; i < N ; i++){
			manager.write(buf, new ByteWrapper(array));
			len = buf.position();
			buf.flip();
			@SuppressWarnings("unused")
            ByteWrapper bw = (ByteWrapper) manager.read(buf);
			buf.clear();
		}
		t2 = System.currentTimeMillis();

		LOG.info("Time to serde ByteWrapper "+N+" times="+(t2-t1)+" ms. Binary size of obj ="+len);
		
		manager.registerDefault(StringWrapper.class);
		
		t1 = System.currentTimeMillis();
		len = 0;
		for(int i=0; i < N ; i++){
			manager.write(buf, new StringWrapper(s));
			len = buf.position();
			buf.flip();
			@SuppressWarnings("unused")
            StringWrapper sw = (StringWrapper) manager.read(buf);
			buf.clear();
		}
		t2 = System.currentTimeMillis();

		LOG.info("Time to serde StringWrapper "+N+" times="+(t2-t1)+" ms. Binary size of obj ="+len);
		
		Kryo kryo = new Kryo();
		
		kryo.register(StringWrapper.class);
		
		t1 = System.currentTimeMillis();
		len = 0;
		for(int i=0; i < N ; i++){
			kryo.writeClassAndObject(buf, new StringWrapper(s));
			len = buf.position();
			buf.flip();
			@SuppressWarnings("unused")
            StringWrapper sw = (StringWrapper) kryo.readClassAndObject(buf);
			buf.clear();
		}
		t2 = System.currentTimeMillis();

		
		
		LOG.info("Time to Kryo StringWrapper "+N+" times="+(t2-t1)+" ms. Binary size of obj ="+len);
		
		manager.registerDefault(HashMap.class);
		
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("1", "1");
		map.put("2", "2");
		map.put("3", "3");
		map.put("4", "4");
		map.put("5", "5");
		map.put("6", "6");
		map.put("7", "7");
		map.put("8", "8");
		map.put("9", "9");
		map.put("10", "10");
		
		t1 = System.currentTimeMillis();
		len = 0;
		for(int i=0; i < N ; i++){
			manager.write(buf, map);
			len = buf.position();
			buf.flip();
			@SuppressWarnings("unused")
			HashMap<String, String> sw = (HashMap<String, String>) manager.read(buf);
			buf.clear();
		}
		t2 = System.currentTimeMillis();
		LOG.info("Time to Kryo HashMap "+N+" times="+(t2-t1)+" ms. Binary size of obj ="+len);
		
		//		Collection<Serializer> serdes = manager.getRegisteredSerializers();
//		for(Serializer ser: serdes)
//		{
//			manager.unregisterSerializer(ser);
//		}
//		
//		
//		LOG.info("Default serialization");
//		t1 = System.currentTimeMillis();
//		
//		for(int i=0; i < N ; i++){
//			manager.write(buf, array);
//			buf.flip();
//			array = (byte[])manager.read(buf, array);
//			buf.clear();
//		}
//		t2 = System.currentTimeMillis();
//		
//		LOG.info("Time to serde 1K array "+N+" times="+(t2-t1)+" ms");
//
//		
//		t1 = System.currentTimeMillis();
//		
//		for(int i=0; i < N ; i++){
//			manager.write(buf, s);
//			buf.flip();
//			String ss = (String) manager.read(buf);
//			buf.clear();
//		}
//		t2 = System.currentTimeMillis();
//
//		LOG.info("Time to serde string "+N+" times="+(t2-t1)+" ms");
		
		
	}
}

class ByteWrapper{

	public byte[] data;
	
	public ByteWrapper(){}
	
	public ByteWrapper(byte[] data)
	{
		this.data = data;
	}
	
	public void setData(byte[] data)
	{
		this.data = data;
	}
	
}


class StringWrapper{
	public String str;
	
	public StringWrapper(){}
	public StringWrapper(String str)
	{
		this.str = str;
	}
}