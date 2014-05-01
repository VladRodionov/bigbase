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
 * Used when serious errors (unchecked exception) in LibLoader are observed.
 *  
 */

public class KodaError extends Error
{
    /**
     * 
     */
    private static final long    serialVersionUID = 1L;

    public final KodaErrorCode errorCode;

    public KodaError(KodaErrorCode code) {
        super();
        this.errorCode = code;
    }

    public KodaError(KodaErrorCode code, Error e) {
        super(e);
        this.errorCode = code;
    }

    public KodaError(KodaErrorCode code, String message) {
        super(message);
        this.errorCode = code;
    }

    @Override
    public String getMessage() {
        return String.format("[%s] %s", errorCode.name(), super.getMessage());
    }

}

