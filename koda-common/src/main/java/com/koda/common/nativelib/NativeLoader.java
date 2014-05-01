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

import java.util.HashMap;

public class NativeLoader
{
    private static HashMap<String, Boolean> loadedLibFiles = new HashMap<String, Boolean>();
    private static HashMap<String, Boolean> loadedLib      = new HashMap<String, Boolean>();

    public static synchronized void load(String lib) {
        if (loadedLibFiles.containsKey(lib) && loadedLibFiles.get(lib) == true)
            return;

        try {
            System.load(lib);
            loadedLibFiles.put(lib, true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized void loadLibrary(String libname) {
        if (loadedLib.containsKey(libname) && loadedLib.get(libname) == true)
            return;

        try {
            System.loadLibrary(libname);
            loadedLib.put(libname, true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
