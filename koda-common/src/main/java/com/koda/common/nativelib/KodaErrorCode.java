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
package com.koda.common.nativelib;

/**
 * Error codes of snappy-java
 * 
 * 
 */
public enum KodaErrorCode {

    // DO NOT change these error code IDs because these numbers are used inside SnappyNative.cpp
    UNKNOWN(0),
    FAILED_TO_LOAD_NATIVE_LIBRARY(1),
    PARSING_ERROR(2),
    NOT_A_DIRECT_BUFFER(3),
    OUT_OF_MEMORY(4),
    FAILED_TO_UNCOMPRESS(5);

    public final int id;

    private KodaErrorCode(int id) {
        this.id = id;
    }

    public static KodaErrorCode getErrorCode(int id) {
        for (KodaErrorCode code : KodaErrorCode.values()) {
            if (code.id == id)
                return code;
        }
        return UNKNOWN;
    }

    public static String getErrorMessage(int id) {
        return getErrorCode(id).name();
    }
}

