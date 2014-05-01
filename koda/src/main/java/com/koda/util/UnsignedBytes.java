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

/*
 * Copyright (C) 2009 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;

import sun.misc.Unsafe;

//import com.google.common.primitives.SignedBytes;

// TODO: Auto-generated Javadoc
/**
 * Static utility methods pertaining to {@code byte} primitives that interpret
 * values as <i>unsigned</i> (that is, any negative value {@code b} is treated
 * as the positive value {@code 256 + b}). The corresponding methods that treat
 * the values as signed are found in {@link SignedBytes}, and the methods for
 * which signedness is not an issue are in {@link Bytes}.
 *
 * @author Kevin Bourrillion
 * @author Martin Buchholz
 * @author Hiroshi Yamauchi
 * @since Guava release 01
 */
public final class UnsignedBytes {
  
	/** The Constant LONG_BYTES. */
	final static int LONG_BYTES = 8;
	
	/**
	 * Instantiates a new unsigned bytes.
	 */
	private UnsignedBytes() {}

  /**
   * Returns the value of the given byte as an integer, when treated as
   * unsigned. That is, returns {@code value + 256} if {@code value} is
   * negative; {@code value} itself otherwise.
   *
   * @param value the value
   * @return the int
   * @since Guava release 06
   */
  public static int toInt(byte value) {
    return value & 0xFF;
  }

  /**
   * Returns the {@code byte} value that, when treated as unsigned, is equal to.
   *
   * @param value a value between 0 and 255 inclusive
   * @return the {@code byte} value that, when treated as unsigned, equals
   * {@code value}, if possible.
   * {@code value}
   */
//  public static byte checkedCast(long value) {
//    checkArgument(value >> 8 == 0, "out of range: %s", value);
//    return (byte) value;
//  }

  /**
   * Returns the {@code byte} value that, when treated as unsigned, is nearest
   * in value to {@code value}.
   *
   * @param value any {@code long} value
   * @return {@code (byte) 255} if {@code value >= 255}, {@code (byte) 0} if
   *     {@code value <= 0}, and {@code value} cast to {@code byte} otherwise
   */
  public static byte saturatedCast(long value) {
    if (value > 255) {
      return (byte) 255; // -1
    }
    if (value < 0) {
      return (byte) 0;
    }
    return (byte) value;
  }

  /**
   * Compares the two specified {@code byte} values, treating them as unsigned
   * values between 0 and 255 inclusive. For example, {@code (byte) -127} is
   * considered greater than {@code (byte) 127} because it is seen as having
   * the value of positive {@code 129}.
   *
   * @param a the first {@code byte} to compare
   * @param b the second {@code byte} to compare
   * @return a negative value if {@code a} is less than {@code b}; a positive
   *     value if {@code a} is greater than {@code b}; or zero if they are equal
   */
  public static int compare(byte a, byte b) {
    return toInt(a) - toInt(b);
  }

  /**
   * Returns the least value present in {@code array}.
   *
   * @param array a <i>nonempty</i> array of {@code byte} values
   * @return the value present in {@code array} that is less than or equal to
   * every other value in the array
   */
  public static byte min(byte... array) {
    //checkArgument(array.length > 0);
    int min = toInt(array[0]);
    for (int i = 1; i < array.length; i++) {
      int next = toInt(array[i]);
      if (next < min) {
        min = next;
      }
    }
    return (byte) min;
  }

  /**
   * Returns the greatest value present in {@code array}.
   *
   * @param array a <i>nonempty</i> array of {@code byte} values
   * @return the value present in {@code array} that is greater than or equal
   * to every other value in the array
   */
  public static byte max(byte... array) {
   // checkArgument(array.length > 0);
    int max = toInt(array[0]);
    for (int i = 1; i < array.length; i++) {
      int next = toInt(array[i]);
      if (next > max) {
        max = next;
      }
    }
    return (byte) max;
  }

  /**
   * Returns a string containing the supplied {@code byte} values separated by.
   *
   * @param separator the text that should appear between consecutive values in
   * the resulting string (but not at the start or end)
   * @param array an array of {@code byte} values, possibly empty
   * @return the string
   * {@code separator}. For example, {@code join(":", (byte) 1, (byte) 2,
   * (byte) 255)} returns the string {@code "1:2:255"}.
   */
  public static String join(String separator, byte... array) {
    //checkNotNull(separator);
    if (array.length == 0) {
      return "";
    }

    // For pre-sizing a builder, just get the right order of magnitude
    StringBuilder builder = new StringBuilder(array.length * 5);
    builder.append(toInt(array[0]));
    for (int i = 1; i < array.length; i++) {
      builder.append(separator).append(toInt(array[i]));
    }
    return builder.toString();
  }

  /**
   * Returns a comparator that compares two {@code byte} arrays
   * lexicographically. That is, it compares, using {@link
   * #compare(byte, byte)}), the first pair of values that follow any common
   * prefix, or when one array is a prefix of the other, treats the shorter
   * array as the lesser. For example, {@code [] < [0x01] < [0x01, 0x7F] <
   * [0x01, 0x80] < [0x02]}. Values are treated as unsigned.
   * 
   * <p>The returned comparator is inconsistent with {@link
   * Object#equals(Object)} (since arrays support only identity equality), but
   * it is consistent with {@link java.util.Arrays#equals(byte[], byte[])}.
   *
   * @return the comparator
   * @see <a href="http://en.wikipedia.org/wiki/Lexicographical_order">
   * Lexicographical order article at Wikipedia</a>
   * @since Guava release 02
   */
  public static Comparator<byte[]> lexicographicalComparator() {
    return LexicographicalComparatorHolder.BEST_COMPARATOR;
  }

  
  /**
   * Lexicographical comparator java impl.
   *
   * @return the comparator
   */
  static Comparator<byte[]> lexicographicalComparatorJavaImpl() {
    return LexicographicalComparatorHolder.PureJavaComparator.INSTANCE;
  }

  /**
   * Provides a lexicographical comparator implementation; either a Java
   * implementation or a faster implementation based on {@link Unsafe}.
   *
   * <p>Uses reflection to gracefully fall back to the Java implementation if
   * {@code Unsafe} isn't available.
   */
 
  static class LexicographicalComparatorHolder {
    
    /** The Constant UNSAFE_COMPARATOR_NAME. */
    static final String UNSAFE_COMPARATOR_NAME =
        LexicographicalComparatorHolder.class.getName() + "$UnsafeComparator";

    /** The Constant BEST_COMPARATOR. */
    static final Comparator<byte[]> BEST_COMPARATOR = getBestComparator();

   
    /**
     * The Enum UnsafeComparator.
     */
    enum UnsafeComparator implements Comparator<byte[]> {
      
      /** The INSTANCE. */
      INSTANCE;

      /** The Constant littleEndian. */
      static final boolean littleEndian =
          ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

      /*
       * The following static final fields exist for performance reasons.
       *
       * In UnsignedBytesBenchmark, accessing the following objects via static
       * final fields is the fastest (more than twice as fast as the Java
       * implementation, vs ~1.5x with non-final static fields, on x86_32)
       * under the Hotspot server compiler. The reason is obviously that the
       * non-final fields need to be reloaded inside the loop.
       *
       * And, no, defining (final or not) local variables out of the loop still
       * isn't as good because the null check on the theUnsafe object remains
       * inside the loop and BYTE_ARRAY_BASE_OFFSET doesn't get
       * constant-folded.
       *
       * The compiler can treat static final fields as compile-time constants
       * and can constant-fold them while (final or not) local variables are
       * run time values.
       */

      /** The unsafe. */
      static final Unsafe theUnsafe;

      /** The offset to the first element in a byte array. */
      static final int BYTE_ARRAY_BASE_OFFSET;

      static {
        theUnsafe = (Unsafe) AccessController.doPrivileged(
            new PrivilegedAction<Object>() {
              @Override
              public Object run() {
                try {
                  Field f = Unsafe.class.getDeclaredField("theUnsafe");
                  f.setAccessible(true);
                  return f.get(null);
                } catch (NoSuchFieldException e) {
                  // It doesn't matter what we throw;
                  // it's swallowed in getBestComparator().
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

      /**
       * Returns true if x1 is less than x2, when both values are treated as
       * unsigned.
       *
       * @param x1 the x1
       * @param x2 the x2
       * @return true, if successful
       */
      // TODO(kevinb): Should be a common method in primitives.UnsignedLongs.
      static boolean lessThanUnsigned(long x1, long x2) {
        return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
      }

      /* (non-Javadoc)
       * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
       */
      @Override public int compare(byte[] left, byte[] right) {
        int minLength = Math.min(left.length, right.length);
        int minWords = minLength / LONG_BYTES;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 64-bit.
         */
        for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
          long lw = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET + (long) i);
          long rw = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET + (long) i);
          long diff = lw ^ rw;

          if (diff != 0) {
            if (!littleEndian) {
              return lessThanUnsigned(lw, rw) ? -1 : 1;
            }

            // Use binary search
            int n = 0;
            int y;
            int x = (int) diff;
            if (x == 0) {
              x = (int) (diff >>> 32);
              n = 32;
            }

            y = x << 16;
            if (y == 0) {
              n += 16;
            } else {
              x = y;
            }

            y = x << 8;
            if (y == 0) {
              n += 8;
            }
            return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
          }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * LONG_BYTES; i < minLength; i++) {
          int result = UnsignedBytes.compare(left[i], right[i]);
          if (result != 0) {
            return result;
          }
        }
        return left.length - right.length;
      }
    }

    /**
     * The Enum PureJavaComparator.
     */
    enum PureJavaComparator implements Comparator<byte[]> {
      
      /** The INSTANCE. */
      INSTANCE;

      /* (non-Javadoc)
       * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
       */
      @Override public int compare(byte[] left, byte[] right) {
        int minLength = Math.min(left.length, right.length);
        for (int i = 0; i < minLength; i++) {
          int result = UnsignedBytes.compare(left[i], right[i]);
          if (result != 0) {
            return result;
          }
        }
        return left.length - right.length;
      }
    }

    /**
     * Returns the Unsafe-using Comparator, or falls back to the pure-Java
     * implementation if unable to do so.
     *
     * @return the best comparator
     */
    static Comparator<byte[]> getBestComparator() {
      try {
        Class<?> theClass = Class.forName(UNSAFE_COMPARATOR_NAME);

        // yes, UnsafeComparator does implement Comparator<byte[]>
        @SuppressWarnings("unchecked")
        Comparator<byte[]> comparator =
            (Comparator<byte[]>) theClass.getEnumConstants()[0];
        return comparator;
      } catch (Throwable t) { // ensure we really catch *everything*
        return lexicographicalComparatorJavaImpl();
      }
    }
  }
}

