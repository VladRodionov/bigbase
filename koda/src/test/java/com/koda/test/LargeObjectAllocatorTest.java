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

import com.koda.NativeMemory;

import java.util.Arrays;

/**
 * Small and Large object allocation uses different strategies.
 * Test both cases below
 *
 * User: Andrew Sokolnikov
 * Date: 11/7/13
 */
public class LargeObjectAllocatorTest {

    private static final int SMALL_OBJECT_SIZE = 128;
    private static final int LARGE_OBJECT_SIZE = 128 * 1024 * 1024;

    public static void main(String[] args) {

        // small object
        allocPutGetCompareFree(SMALL_OBJECT_SIZE);

        // large object
        allocPutGetCompareFree(LARGE_OBJECT_SIZE);

    }

    private static void allocPutGetCompareFree(int size) {
        // alloc
        long smptr = NativeMemory.malloc(size);
        // put
        //long smptrl = NativeMemory.lockAddress(smptr);
        byte[] smbuf = new byte[size];
        NativeMemory.memcpy(smbuf, 0, size, smptr, 0);
        //NativeMemory.unlockAddress(smptrl);
        // get
        //long smptrl2 = NativeMemory.lockAddress(smptr);
        byte[] smbuf2 = new byte[size];
        NativeMemory.memcpy(smptr, 0, smbuf2, 0, size);
        //NativeMemory.unlockAddress(smptrl2);
        // compare
        boolean equal = Arrays.equals(smbuf, smbuf2);
        assert equal == true;
        // free
        NativeMemory.free(smptr);
    }

}
