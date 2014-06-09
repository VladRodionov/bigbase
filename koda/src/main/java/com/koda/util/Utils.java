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
package com.koda.util;

import java.nio.ByteBuffer;

import sun.misc.Unsafe;

import com.koda.NativeMemory;
import com.koda.NativeMemoryException;

// TODO: Auto-generated Javadoc
/**
 * The Class Utils.
 */
public class Utils {

	
	/**
	 * Murmur Hash 1.
	 *
	 * @param data the data
	 * @param seed the seed
	 * @return the int
	 */
	  public static int hash(byte[] data,  int seed) {
		    int m = 0x5bd1e995;
		    int r = 24;
		    int length = data.length;
		    int h = seed ^ length;

		    int len_4 = length >> 2;

		    for (int i = 0; i < len_4; i++) {
		      int i_4 = i << 2;
		      int k = data[i_4 + 3];
		      k = k << 8;
		      k = k | (data[i_4 + 2] & 0xff);
		      k = k << 8;
		      k = k | (data[i_4 + 1] & 0xff);
		      k = k << 8;
		      k = k | (data[i_4 + 0] & 0xff);
		      k *= m;
		      k ^= k >>> r;
		      k *= m;
		      h *= m;
		      h ^= k;
		    }

		    // avoid calculating modulo
		    int len_m = len_4 << 2;
		    int left = length - len_m;

		    if (left != 0) {
		      if (left >= 3) {
		        h ^= data[length - 3] << 16;
		      }
		      if (left >= 2) {
		        h ^= data[length - 2] << 8;
		      }
		      if (left >= 1) {
		        h ^= data[length - 1];
		      }

		      h *= m;
		    }

		    h ^= h >>> 13;
		    h *= m;
		    h ^= h >>> 15;

		    // This is a stupid thinh I have ever stuck upon
		    if(h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE +1);
		    return h;
		  }

	  
	  /**
  	 * Hash.
  	 *
  	 * @param data the data
  	 * @param offset the offset
  	 * @param length the length
  	 * @param seed the seed
  	 * @return the int
  	 */
  	public static int hash(ByteBuffer data, final int offset, final int length,  final int seed) {

  		
		if(data.isDirect() == false){    
  		int m = 0x5bd1e995;
		    int r = 24;
		    int h = seed ^ length;
		    data.position(offset);
		    int len_4 = length >> 2;

		    for (int i = 0; i < len_4; i++) {
		      int i_4 = i << 2;
		      int k = data.get(offset + i_4 + 3);
		      k = k << 8;
		      k = k | (data.get(offset + i_4 + 2) & 0xff);
		      k = k << 8;
		      k = k | (data.get(offset + i_4 + 1) & 0xff);
		      k = k << 8;
		      k = k | (data.get(offset+ i_4 + 0) & 0xff);
		      k *= m;
		      k ^= k >>> r;
		      k *= m;
		      h *= m;
		      h ^= k;
		    }

		    // avoid calculating modulo
		    int len_m = len_4 << 2;
		    int left = length - len_m;

		    if (left != 0) {
		      if (left >= 3) {
		        h ^= data.get(offset+length - 3) << 16;
		      }
		      if (left >= 2) {
		        h ^= data.get(offset+length - 2) << 8;
		      }
		      if (left >= 1) {
		        h ^= data.get(offset+length - 1);
		      }

		      h *= m;
		    }

		    h ^= h >>> 13;
		    h *= m;
		    h ^= h >>> 15;
			// This is a stupid thing I have ever stuck upon
			if(h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE +1);
		    return h;
		} else{
  			// Direct buffer
			int h = murmur3hashNative(data, offset, length, seed);
  			if(h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE +1);
  			return h;
		}
  	}

 
  	
 
  	
  		/**
		   * Hash_murmur3.
		   *
		   * @param key the key
		   * @param offset the offset
		   * @param len the len
		   * @param seed the seed
		   * @return the int
		   */
		  public static int hash_murmur3(ByteBuffer key, int offset, int len, int seed )
  		{
 
  			int h = murmur3hashNative(key, offset, len, seed);
  			if(h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE +1);
  			return h;
  		}
 

		  
  		/**
		   * Hash_murmur3.
		   *
		   * @param ptr the ptr
		   * @param offset the offset
		   * @param len the len
		   * @param seed the seed
		   * @return the int
		   */
		  public static int hash_murmur3(long ptr, int offset, int len, int seed )
  		{
  			int h = murmurHash(ptr, offset, len, seed);
  			if(h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE +1);
  			return h;
  		}
  		
//          public static void hash_murmur3_128(long ptr, int offset, int len, int seed, byte[] out )
//          {
//              murmur3hashNativePtr128(ptr, offset, len, seed, out);
//  
//          }
//          
//          public static void hash_murmur3_128(long ptr, int offset, int len, int seed, long out )
//          {
//              murmur3hashNativePtrPtr128(ptr, offset, len, seed, out);
//  
//          }
  		/**
		   * Murmur3hash native.
		   *
		   * @param key the key
		   * @param offset the offset
		   * @param len the len
		   * @param seed the seed
		   * @return the int
		   */
		  private static int murmur3hashNative(ByteBuffer key, int offset, int len, int seed)
		  {
			  if( key.isDirect()){
				  long ptr = NativeMemory.getBufferAddress(key);
				  return murmurHash(ptr, offset, len, seed);
			  } else{
				  throw new RuntimeException("heap bytebuffer");
			  }
			  
		  }
  	
  		/**
		   * Murmur3hash native ptr.
		   *
		   * @param ptr the ptr
		   * @param offset the offset
		   * @param len the len
		   * @param seed the seed
		   * @return the int
		   */
		  private static int murmurHash(long ptr, int offset, int len, int seed)
		  {
			  Unsafe unsafe = NativeMemory.getUnsafe();
			  
			    final int m = 0x5bd1e995;
			    final int r = 24;
			    final int length = len;
			    int h = seed ^ length;

			    final int len_4 = length >> 2;
		      	ptr += offset;
			    for (int i = 0; i < len_4; i++) {
			      int i_4 = i << 2;
			      int k = unsafe.getByte(ptr + i_4 + 3) & 0xff;
			      k = k << 8;
			      k = k | (unsafe.getByte(ptr + i_4 + 2) & 0xff);
			      k = k << 8;
			      k = k | (unsafe.getByte(ptr + i_4 + 1) & 0xff);
			      k = k << 8;
			      k = k | (unsafe.getByte(ptr + i_4 ) & 0xff);
			      k *= m;
			      k ^= k >>> r;
			      k *= m;
			      h *= m;
			      h ^= k;
			    }

			    // avoid calculating modulo
			    int len_m = len_4 << 2;
			    int left = length - len_m;

			    if (left != 0) {
			      if (left >= 3) {
			        h ^= (unsafe.getByte( ptr + length - 3) & 0xff) << 16;
			      }
			      if (left >= 2) {
			        h ^= (unsafe.getByte( ptr + length - 2) & 0xff) << 8;
			      }
			      if (left >= 1) {
			        h ^= (unsafe.getByte( ptr + length - 1) & 0xff);
			      }

			      h *= m;
			    }

			    h ^= h >>> 13;
			    h *= m;
			    h ^= h >>> 15;

			    // This is a stupid thinh I have ever stuck upon
			    if(h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE +1);
			    return h;
			  
		  }
  		
  	
	        /**
           * Murmur3hash native.
           *
           * @param key the key
           * @param offset the offset
           * @param len the len
           * @param seed the seed
           * @return the int
           */

//        private static native void murmur3hashNative128(ByteBuffer key, int offset, int len, int seed, byte[] hash);
//    
//        /**
//           * Murmur3hash native ptr.
//           *
//           * @param ptr the ptr
//           * @param offset the offset
//           * @param len the len
//           * @param seed the seed
//           * @return the int
//           */
//
//        private static native void murmur3hashNativePtr128(long ptr, int offset, int len, int seed, byte[] out);
//        /**
//         * 
//         * @param ptr
//         * @param offset
//         * @param len
//         * @param seed
//         * @param hash
//         */
//        private static native void murmur3hashNativePtrPtr128(long ptr, int offset, int len, int seed, long out);
    		  
		  
		  
		/**
		 * Murmur Hash 1.
		 *
		 * @param data the data
		 * @param seed the seed
		 * @return the int
		 */
		  public static int hashString(String data,  int seed) {
			    int m = 0x5bd1e995;
			    int r = 24;
			    int length = data.length();
			    int h = seed ^ length;

			    int len_4 = length >> 2;

			    for (int i = 0; i < len_4; i++) {
			      int i_4 = i << 2;
			      int k = (byte)data.charAt(i_4 + 3);
			      k = k << 8;
			      k = k | (byte)(data.charAt(i_4 + 2) & 0xff);
			      k = k << 8;
			      k = k | (byte)(data.charAt(i_4 + 1) & 0xff);
			      k = k << 8;
			      k = k | (byte)(data.charAt(i_4) & 0xff);
			      k *= m;
			      k ^= k >>> r;
			      k *= m;
			      h *= m;
			      h ^= k;
			    }

			    // avoid calculating modulo
			    int len_m = len_4 << 2;
			    int left = length - len_m;

			    if (left != 0) {
			      if (left >= 3) {
			        h ^= ((byte)data.charAt(length - 3)) << 16;
			      }
			      if (left >= 2) {
			        h ^= ((byte)data.charAt(length - 2)) << 8;
			      }
			      if (left >= 1) {
			        h ^= ((byte)data.charAt(length - 1));
			      }

			      h *= m;
			    }

			    h ^= h >>> 13;
			    h *= m;
			    h ^= h >>> 15;
				// This is a stupid thinh I have ever stuck upon
				if(h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE +1);
			    return h;
			  }
		
		/**
		 * The main method.
		 *
		 * @param args the arguments
		 * @throws NativeMemoryException the native memory exception
		 */
		public static void main(String[] args) throws NativeMemoryException
		{

			byte[] buf = new byte[16];
			
			
			System.out.println("hash="+ hash(buf,0));
			
			long t1 = System.currentTimeMillis();
			int k =0;
			for(int i=0; i < 1000000; i++){
				k |= hash(buf, i);
			}
			long t2 = System.currentTimeMillis();
			
			System.out.println("Time to hash 1M 16 bytes="+(t2-t1));

			String s = "1234567890abcdef";
			
			t1 = System.currentTimeMillis();
			k =0;
			for(int i=0; i < 1000000; i++){
				k |= hashString(s, i);
			}
			t2 = System.currentTimeMillis();
			
			System.out.println("Time to hash 1M 16 chars="+(t2-t1)+" hash ="+hashString(s,0));
			
			t1 = System.currentTimeMillis();
			k =0;
			for(int i=0; i < 1000000; i++){
				k |= hash(s.getBytes(), i);
			}
			t2 = System.currentTimeMillis();
			
			System.out.println("Time to hash 1M 16 chars/bytes="+(t2-t1)+" hash="+hash(s.getBytes(), 0));
			
			ByteBuffer bbuf = NativeMemory.allocateDirectBuffer(16, 256);
			long ptr = NativeMemory.getBufferAddress(bbuf);
			t1 = System.currentTimeMillis();
			for(int i=0; i < 10000000; i++)
			{
				k |= Utils.hash_murmur3(ptr, 0, 16, 0);
			}
			
			t2 = System.currentTimeMillis();
			System.out.println("Time to hash 10M 16 chars/bytes native (ptr)="+(t2-t1)+" hash="+k);
			
			t1 = System.currentTimeMillis();
			int kk =0;
			for(int i=0; i < 100000000; i++)
			{
				//kk |= Utils.hash(bbuf, 0, 16, 0);
//				bbuf.position(0);
//				bbuf.putInt(kk);
//				bbuf.putInt(kk);
				kk |= Utils.hash_murmur3(ptr, 0, 16, 0);
			}
			
			t2 = System.currentTimeMillis();
			System.out.println("Time to hash 100M 16 bytes  ByteBuffer="+(t2-t1)+" hash="+kk);

		}

		/**
		 * Search key in a buffer
		 * 
		 * @param memory
		 * @param limit
		 * @param keyPtr
		 * @param keySize
		 * @return
		 */
//		public static native int search(long memory, int limit, long keyPtr, int keySize);
		
//		public static native int searchOpt(long memory, int limit, long keyPtr, int keySize, boolean sorted);
		
//		public static native int searchOptFixedRecordSize(long memory, int limit, long keyPtr, int keySize, int recordSize, boolean sorted);
//		
//		public static native int compareKeys(long memfirst, long memsecond);
//		
//        public static native int compareDefault(long memfirst, long memsecond);	
//        
//        public static native int compareReverse(long memfirst, long memsecond);
//		/**
//		 * Finds index of a key in ordered set of keys
//		 * each key is :
//		 * [len][data]
//		 * [len] - 1 or 2 bytes byte
//		 * 
//		 * @param memory
//		 * @param key
//		 * @param size
//		 * @return
//		 */
//        public static native int findPosition(long memory, int limit, long key, int size);
//        
//        public static native int findPositionDirect(long memory, int limit, long key, int size);
//        
//        public static native int findPositionDirectFixed(long memory, int limit, int ksize, long key, int size);
        
        public static void memset(long origin, int offset, int limit, int value)
        {
 
                long size = NativeMemory.mallocUsableSize(origin);
                if(offset + limit > size) throw new RuntimeException("memory corruption");
                if(offset + limit < 0) throw new RuntimeException("memory corruption");
                memset(origin+ offset, limit, value);
            
            
        }
//        private static native void memset(long memory, int limit, int value);
//        
//		public static native int prefixlen(long[] memptr, short[] sizes, int size);
		
        private static void memset(long l, int limit, int value) {
			// TODO Auto-generated method stub
			
		}


		/**
		 * Equals.
		 *
		 * @param s the s
		 * @param key the key
		 * @return true, if successful
		 */
		public final static boolean equals(final byte[] s, final byte[] key) {
			if(key.length != s.length) return false;
			for(int i=0; i < s.length; i++)
			{
				if(s[i] != key[i]) {
				  System.out.println("Failed at "+i+" of "+s.length);
				  return false;
				}
			}
			return true;
		}


		/**
		 * Equals.
		 *
		 * @param keyValue the key value
		 * @param offset the offset
		 * @param keyLen the key len
		 * @param otherKey the other key
		 * @return true, if successful
		 */
		public static boolean equals(final ByteBuffer keyValue, final int offset, final int keyLen,
				final byte[] otherKey) {
			for(int i=offset; i < offset+keyLen; i++){
				if(keyValue.get(i) != otherKey[i-offset]) return false;
			}
			return true;
		}


		public static int findPositionDirectFixed(long l, int i, int keySize,
				long searchKey, int j) {
			// TODO - implementation
			return 0;
		}


		public static int compareDefault(long memfirst, long memsecond) {
			// TODO Auto-generated method stub
			return 0;
		}


		public static int compareKeys(long memfirst, long memsecond) {
			// TODO Auto-generated method stub
			return 0;
		}


		public static int compareReverse(long memfirst, long memsecond) {
			// TODO Auto-generated method stub
			return 0;
		}


		public static void hash_murmur3_128(ByteBuffer buf, int offset,
				int size, int i, byte[] result) 
		{
			// TODO Auto-generated method stub
			
		}


		public static short prefixlen(long[] pointers, short[] sizes,
				int numEntries) {
			// TODO Auto-generated method stub
			return 0;
		}


		public static int searchOpt(long l, int committedSize, long keyPtr,
				int keySize, boolean sorted) {
			// TODO Auto-generated method stub
			return 0;
		}
	
//		public static boolean equals(final ByteBuffer keyValue, final int offset, final int keyLen,
//				final byte[] otherKey) {
//			for(int i=offset; i < offset+keyLen; i++){
//				if(keyValue.get(i) != otherKey[i-offset]) return false;
//			}
//			return true;
//		}
		
}
