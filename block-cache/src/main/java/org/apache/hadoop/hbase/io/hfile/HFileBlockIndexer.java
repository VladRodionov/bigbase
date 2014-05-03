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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableUtils;

import sun.misc.Unsafe;

public class HFileBlockIndexer {
  public final static int NOT_FOUND = -1;
  public final static int ILLEGAL_STATE = -2;
  public static final Unsafe theUnsafe;
  static int MAX_SCAN_SIZE = 5;

  /** The offset to the first element in a byte array. */
  public static final int BYTE_ARRAY_BASE_OFFSET;
  public static final boolean littleEndian = 
    ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
  static {
    theUnsafe = (Unsafe) AccessController
        .doPrivileged(new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            try {
              Field f = Unsafe.class.getDeclaredField("theUnsafe");
              f.setAccessible(true);
              return f.get(null);
            } catch (NoSuchFieldException e) {
              // It doesn't matter what we throw;
              // it's swallowed in getBestComparer().
              throw new Error();
            } catch (IllegalAccessException e) {
              throw new Error();
            }
          }
        });

    BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

    // sanity check - this should never fail
    if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
      throw new AssertionError();
    }
  }

  public static int[] createIndex(ByteBuffer block, boolean useMemstoreTs,
      boolean useTags) throws IOException {

    int startPosition = block.position();
    int limit = block.limit();
    
    List<Integer> list = new ArrayList<Integer>();
    byte[] buffer = block.array();
    int arrayOffset = block.arrayOffset();
    int off = block.position() + arrayOffset;
    while (block.hasRemaining()) {
      // Add current KV offset (relative)
      list.add(off - arrayOffset);
      int keyLen = theUnsafe
          .getInt(buffer, (long) BYTE_ARRAY_BASE_OFFSET + off);
      int valLen = theUnsafe.getInt(buffer, (long) BYTE_ARRAY_BASE_OFFSET + off
          + Bytes.SIZEOF_INT);
      if(littleEndian){
        keyLen = Integer.reverseBytes(keyLen);
        valLen = Integer.reverseBytes(valLen);
      }
      int skip = keyLen + valLen + 2 * Bytes.SIZEOF_INT;
      if (useTags) {
        short tagsLen = theUnsafe.getShort(buffer,
            (long) BYTE_ARRAY_BASE_OFFSET + off + skip);
        if(littleEndian){
          tagsLen = Short.reverseBytes(tagsLen);
        }
        
        skip += tagsLen + /*KeyValue.TAGS_LENGTH_SIZE*/ Bytes.SIZEOF_SHORT /* size of short */;
      }
      if (useMemstoreTs) {
        // TODO decodeMemstoreTS?
        long memstoreTS = Bytes.readVLong(buffer, off + skip);
        int memstoreTSLen = WritableUtils.getVIntSize(memstoreTS);
        skip += memstoreTSLen;
      }
      off += skip;
      block.position(off - arrayOffset);
    }

    int[] index = new int[list.size()];
    for (int i = 0; i < index.length; i++) {
      index[i] = list.get(i);
    }
    // revert everything back
    block.position(startPosition);
    block.limit(limit);
    
    return index;
  }

  /**
   * Find first row which is GE than a given row. We use default
   * Comparator - KVComparator.COMPARATOR
   * 
   * @param block
   *          - block of KVs
   * @param buf
   *          - row buffer (to find)
   * @param off
   *          - row offset
   * @param len
   *          - row length
   * @param indexData
   *          - blocks index
   * @return offset of a first KV which is GE than a given row or -1 otherwise
   */
  public static int seekAfter(ByteBuffer block, byte[] buf, int off,
      int len, int[] indexData) {

    // TODO check last index first - short-circuit
    byte[] buffer = block.array();
    int arrayOffset = block.arrayOffset();
    int startIndex = findIndex(block.position(), indexData);
    if (startIndex < 0) {
      throw new IllegalStateException(
          "Block index seek. No index for current offset: " + block.position());
    }
    // Check last row in a block
    int rowLen = theUnsafe.getShort(buffer,
        (long) indexData[indexData.length - 1] + arrayOffset
            + BYTE_ARRAY_BASE_OFFSET + 2 * Bytes.SIZEOF_INT);
    
    if(littleEndian){
      rowLen = Short.reverseBytes((short)rowLen);
    }
    
    int rowOffset = indexData[indexData.length - 1] + arrayOffset + 2
        * Bytes.SIZEOF_INT + Bytes.SIZEOF_SHORT;

    if (Bytes.compareTo(buffer, rowOffset, rowLen, buf, off, len) < 0) {
      return -1; // not found
    }

    int endIndex = indexData.length - 1;
    // startIndex always < 'row'
    // endIndex always >= 'row'
    while (endIndex - startIndex > MAX_SCAN_SIZE) {
      int nextIndex = startIndex + (endIndex - startIndex) / 2;
      // compare 'nextIndex' and
      rowLen = theUnsafe.getShort(buffer, (long) indexData[nextIndex]
          + arrayOffset + BYTE_ARRAY_BASE_OFFSET + 2 * Bytes.SIZEOF_INT);
      if(littleEndian){
        rowLen = Short.reverseBytes((short)rowLen);
      }      
      rowOffset = indexData[nextIndex] + arrayOffset + 2 * Bytes.SIZEOF_INT
          + Bytes.SIZEOF_SHORT;

      int result = Bytes.compareTo(buffer, rowOffset, rowLen, buf, off, len);
      if (result < 0) {
        startIndex = nextIndex; // not found
      } else if (result > 0) {
        endIndex = nextIndex;
      } else {
        // found exact match
        return indexData[nextIndex];
      }
    }

    // OK , now do scan until first row which is GE of a given 'row'
    for (int i = startIndex; i <= endIndex; i++) {

      rowLen = theUnsafe.getShort(buffer, (long) indexData[i] + arrayOffset
          + BYTE_ARRAY_BASE_OFFSET + 2 * Bytes.SIZEOF_INT);
      if(littleEndian){
        rowLen = Short.reverseBytes((short)rowLen);
      }
      rowOffset = indexData[i] + arrayOffset + 2 * Bytes.SIZEOF_INT
          + Bytes.SIZEOF_SHORT;

      int result = Bytes.compareTo(buffer, rowOffset, rowLen, buf, off, len);
      if (result >= 0) {
        return indexData[i];
      }

    }

    return -1;
  }
  
 
  /**
   * Seeks row before (strict = true) or before or at (strict = false) the given 'row' using default Comparator
   * 
   * @param block
   * @param buf
   * @param off
   * @param len
   * @param indexData
   * @param strict
   * @return offset of a found row, or -1 if not found or -2 if Illegal search (strict == true and found 
   * exact match on first key in a block)
   * 
   */
//  public static int seekBefore(ByteBuffer block, byte[] buf, int off,
//      int len, int[] indexData, boolean strict) {
//
//    // Cut the length of 'row'
//    off += Bytes.SIZEOF_SHORT;
//    len -= Bytes.SIZEOF_SHORT;
//    
//    // TODO check first  index  first - short-circuit
//    byte[] buffer = block.array();
//    int arrayOffset = block.arrayOffset();
//    /*DEBUG*/ System.out.println("arrayOffset="+arrayOffset+" block.position="+block.position());
//    System.out.println(new String(buf, off,len));
//    int startIndex = findIndex(block.position(), indexData);
//    if (startIndex < 0) {
//      throw new IllegalStateException(
//          "Block index seek. No index for current offset: " + block.position());
//    }
//    int endIndex = indexData.length - 1;
//    // Check first row-key in a block
////    int rowLen = theUnsafe.getShort(buffer,
////        (long) indexData[startIndex] + arrayOffset
////            + BYTE_ARRAY_BASE_OFFSET + 2 * Bytes.SIZEOF_INT);
//    int keyLen = theUnsafe.getInt(buffer,
//        (long) indexData[startIndex] + arrayOffset
//            + BYTE_ARRAY_BASE_OFFSET) - Bytes.SIZEOF_SHORT;
//    
////    if(littleEndian){
////      rowLen = Short.reverseBytes((short)rowLen);
////    }
//    
//    if(littleEndian){
//      keyLen = Integer.reverseBytes(keyLen);
//    }    
////    int rowOffset = indexData[startIndex] + arrayOffset + 2
////        * Bytes.SIZEOF_INT + Bytes.SIZEOF_SHORT;
//    int keyOffset = indexData[startIndex] + arrayOffset + 2
//    * Bytes.SIZEOF_INT + Bytes.SIZEOF_SHORT;    
//
//    /*DEBUG*/ System.out.println(new String(buffer, keyOffset, keyLen));
////    int result = Bytes.compareTo(buffer, rowOffset, rowLen, buf, off, len);
//    int result = Bytes.compareTo(buffer, keyOffset, keyLen, buf, off, len);
//
//    if ( result > 0 /*|| (result == 0 && strict)*/) {
//      // the given row is less than the current left most (smallest) row in a block     
//      return NOT_FOUND; // not found
//    } else if( result == 0 && strict && startIndex == 0){
//      return ILLEGAL_STATE;
//    }
//
//
//    // startIndex always < 'row'
//    // endIndex always >= 'row'
//    // Find the first row which is greater or equal of a given 'row'
//    int foundIndex = -1;
//    boolean exactMatch = false;
//    
//    while (endIndex - startIndex > MAX_SCAN_SIZE) {
//      int nextIndex = startIndex + (endIndex - startIndex) / 2;
//      // compare 'nextIndex' and
//      rowLen = theUnsafe.getShort(buffer, (long) indexData[nextIndex]
//          + arrayOffset + BYTE_ARRAY_BASE_OFFSET + 2 * Bytes.SIZEOF_INT);
//      if(littleEndian){
//        rowLen = Short.reverseBytes((short)rowLen);
//      }      
//      rowOffset = indexData[nextIndex] + arrayOffset + 2 * Bytes.SIZEOF_INT
//          + Bytes.SIZEOF_SHORT;
//
//      result = Bytes.compareTo(buffer, rowOffset, rowLen, buf, off, len);
//      if (result < 0) {
//        startIndex = nextIndex; // not found
//      } else if (result > 0) {
//        endIndex = nextIndex;
//      } else {
//        // found exact match
//        //return indexData[nextIndex];
//        foundIndex = nextIndex;
//        exactMatch = true;
//        break;
//      }
//    }
//
//    if( foundIndex < 0){
//      // OK , now do scan until first row which is GE of a given 'row'
//      for (int i = startIndex; i <= endIndex; i++) {
//
//        rowLen = theUnsafe.getShort(buffer, (long) indexData[i] + arrayOffset
//          + BYTE_ARRAY_BASE_OFFSET + 2 * Bytes.SIZEOF_INT);
//        if(littleEndian){
//          rowLen = Short.reverseBytes((short)rowLen);
//        }
//        rowOffset = indexData[i] + arrayOffset + 2 * Bytes.SIZEOF_INT
//          + Bytes.SIZEOF_SHORT;
//
//        result = Bytes.compareTo(buffer, rowOffset, rowLen, buf, off, len);
//        if (result >= 0) {
//          foundIndex = i;
//          exactMatch = result == 0;
//          break;
//        }
//      }
//    }
//    
//    if( foundIndex < 0){
//      return NOT_FOUND; // Not found
//    } else if (strict == false && exactMatch == true){
//      return indexData[foundIndex];
//    } else if(foundIndex > startIndex){
//      return indexData[foundIndex -1];
//    }
//    // foundIndex == 0 && strict == true
//    return ILLEGAL_STATE;
//  }

  public static int seekBefore(ByteBuffer block, byte[] buf, int off,
      int len, int[] indexData, boolean strict, KVComparator comparator) {

    
    // TODO check first  index  first - short-circuit
    byte[] buffer = block.array();
    int arrayOffset = block.arrayOffset();
    //*DEBUG*/ System.out.println("arrayOffset="+arrayOffset+" block.position="+block.position());
    //System.out.println("look for: "+new String(buf, off,len));
    int startIndex = findIndex(block.position(), indexData);
    if (startIndex < 0) {
      throw new IllegalStateException(
          "Block index seek. No index for current offset: " + block.position());
    }
    int endIndex = indexData.length - 1;
    // Check first row-key in a block

    int keyLen = theUnsafe.getInt(buffer,
        (long) indexData[startIndex] + arrayOffset
            + BYTE_ARRAY_BASE_OFFSET) ;
        
    if(littleEndian){
      keyLen = Integer.reverseBytes(keyLen);
    }    
    //keyLen -= Bytes.SIZEOF_SHORT;
    
    int keyOffset = indexData[startIndex] + arrayOffset + 2
    * Bytes.SIZEOF_INT ;    

    //*DEBUG*/ System.out.println("Start key: "+new String(buffer, keyOffset, keyLen));
    int result = comparator.compare(buffer, keyOffset, keyLen, buf, off, len);

    if ( result > 0 /*|| (result == 0 && strict)*/) {
      // the given row is less than the current left most (smallest) row in a block     
      //*DEBUG*/System.out.println("NOT_FOUND");
      return NOT_FOUND; // not found
    } else if( result == 0 && strict && startIndex == 0){
      //*DEBUG*/System.out.println("ILLEGAL_STATE");

      return ILLEGAL_STATE;
    }


    // startIndex always < 'row'
    // endIndex always >= 'row'
    // Find the first row which is greater or equal of a given 'row'
    int foundIndex = -1;
    boolean exactMatch = false;
    
    while (endIndex - startIndex > MAX_SCAN_SIZE) {
      int nextIndex = startIndex + (endIndex - startIndex) / 2;
      // compare 'nextIndex' and
      keyLen = theUnsafe.getInt(buffer, (long) indexData[nextIndex]
          + arrayOffset + BYTE_ARRAY_BASE_OFFSET );
      if(littleEndian){
        keyLen = Integer.reverseBytes(keyLen);
      }
      
      keyOffset = indexData[nextIndex] + arrayOffset + 2 * Bytes.SIZEOF_INT;
        //  + Bytes.SIZEOF_SHORT;

      result = comparator.compare(buffer, keyOffset, keyLen, buf, off, len);
      if (result < 0) {
        startIndex = nextIndex; // not found
      } else if (result > 0) {
        endIndex = nextIndex;
      } else {
        // found exact match
        //return indexData[nextIndex];
        foundIndex = nextIndex;
        exactMatch = true;
        break;
      }
    }

    if( foundIndex < 0){
      // OK , now do scan until first row which is GE of a given 'row'
      for (int i = startIndex; i <= endIndex; i++) {

        keyLen = theUnsafe.getInt(buffer, (long) indexData[i] + arrayOffset
          + BYTE_ARRAY_BASE_OFFSET );
        if(littleEndian){
          keyLen = Integer.reverseBytes(keyLen);
        }        
        keyOffset = indexData[i] + arrayOffset + 2 * Bytes.SIZEOF_INT;
         // + Bytes.SIZEOF_SHORT;

        result = comparator.compare(buffer, keyOffset, keyLen, buf, off, len);
        if (result >= 0) {
          foundIndex = i;
          exactMatch = result == 0;
          break;
        }
      }
    }
    
    if( foundIndex < 0){
//      endIndex = indexData.length -1;
//      keyLen = theUnsafe.getInt(buffer, (long) indexData[endIndex]
//                                                         + arrayOffset + BYTE_ARRAY_BASE_OFFSET );
//      if(littleEndian){
//        keyLen = Integer.reverseBytes(keyLen);
//      }
//                                                     
//      keyOffset = indexData[endIndex] + arrayOffset + 2 * Bytes.SIZEOF_INT;
//      /*DEBUG*/System.out.println("NOT_FOUND2. last key: "+ new String(buffer, keyOffset, keyLen));
//      /*DEBUG*/ System.out.println("comparator="+ comparator.compare(buffer, keyOffset, keyLen, buf, off, len));

      return NOT_FOUND; // Not found
    } else if (strict == false && exactMatch == true){
      return indexData[foundIndex];
    } else if(foundIndex > startIndex){
      return indexData[foundIndex -1];
    }
    // foundIndex == 0 && strict == true
    //*DEBUG*/System.out.println("ILLEGAL_STATE2: foundIndex="+foundIndex+" startIndex="+startIndex);

    return ILLEGAL_STATE;
  }
  
  
  private static int findIndex(int position, int[] indexData) {

    return Arrays.binarySearch(indexData, position);

  }

}
