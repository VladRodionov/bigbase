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



import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

/**
 * This class loads a native library  xxx (xxx.dll,
 * xxx.so, etc.) according to the user platform (<i>os.name</i> and
 * <i>os.arch</i>). The natively compiled libraries bundled to java jar
 * contain the codes of the original java and JNI programs to access native code.
 * 
 * In default, no configuration is required to use native loader.
 * 
 * This LibLoader searches for native libraries (xxx.dll,
 * xxx.so, etc.) in the following order:
 * <ol>
 * <li>(System property: <i>koda.lib.path</i>)
 * <li>One of the libraries embedded in XXX-java-(version).jar extracted into
 * (System property: <i>java.io.tempdir</i> or if
 * <i>koda.tempdir</i> is set, use this folder.)
 * <li>Folders specified by java.lib.path system property (This is the default
 * path that JVM searches for native libraries)
 * </ol>
 * 
 * <p>
 * If you do not want to use folder <i>java.io.tempdir</i>, set the System
 * property <i>koda.tempdir</i>. For example, to use
 * <i>/tmp/koda</i> as a temporary folder to copy native libraries, use -D option
 * of JVM:
 * 
 * <pre>
 * <code>
 * java -Dkoda.tempdir="/tmp/koda" ...
 * </code>
 * </pre>
 * 
 * </p>
 * 
 * 
 */
public class LibLoader
{
    public static final String     KEY_KODA_LIB_PATH             = "koda.lib.path";
    public static final String     KEY_KODA_TEMPDIR              = "koda.tempdir";
    public static final String     KEY_KODA_USE_SYSTEMLIB        = "koda.use.systemlib";
    public static final String     KEY_KODA_DISABLE_BUNDLED_LIBS = "koda.disable.bundled.libs"; // Depreciated, but preserved for backward compatibility

    private static HashSet<String>         isLoadedSet           =  new HashSet<String>();


    private static ClassLoader getRootClassLoader() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        while (cl.getParent() != null) {
            cl = cl.getParent();
        }
        return cl;
    }

    private static byte[] getByteCode(String resourcePath) throws IOException {

        InputStream in = LibLoader.class.getResourceAsStream(resourcePath);
        if (in == null)
            throw new IOException(resourcePath + " is not found");
        byte[] buf = new byte[1024];
        ByteArrayOutputStream byteCodeBuf = new ByteArrayOutputStream();
        for (int readLength; (readLength = in.read(buf)) != -1;) {
            byteCodeBuf.write(buf, 0, readLength);
        }
        in.close();

        return byteCodeBuf.toByteArray();
    }

    public static boolean isNativeLibraryLoaded(String name) {
        return isLoadedSet.contains(name);
    }

    private static boolean hasInjectedNativeLoader() {
        try {
            final String nativeLoaderClassName = "com.koda.common.nativelib.NativeLoader";
            @SuppressWarnings("unused")
            Class< ? > c = Class.forName(nativeLoaderClassName);
            // If this native loader class is already defined, it means that another class loader already loaded the native library 
            return true;
        }
        catch (ClassNotFoundException e) {
            // do loading
            return false;
        }
    }

    /**
     * Load Native library and its JNI native implementation in the root class
     * loader. This process is necessary to avoid the JNI multi-loading issue
     * when the same JNI library is loaded by different class loaders in the
     * same JVM.
     * 
     * In order to load native code in the root class loader, this method first
     * inject NativeLoader class into the root class loader, because
     * {@link System#load(String)} method uses the class loader of the caller
     * class when loading native libraries.
     * 
     * <pre>
     * (root class loader) -> [NativeLoader (load JNI code), Java Native (has native methods), KodaError, KodaErrorCode]  (injected by this method)
     *    |
     *    |
     * (child class loader) -> Sees the above classes loaded by the root class loader.
     *   Then creates NativeAPI implementation by instantiating SnappyNaitive class.
     * </pre>
     * 
     * 
     * <pre>
     * (root class loader) -> [NativeLoader, Native ...]  -> native code is loaded by once in this class loader 
     *   |   \
     *   |    (child2 class loader)      
     * (child1 class loader)
     * 
     * child1 and child2 share the same Native code loaded by the root class loader.
     * </pre>
     * 
     * Note that Java's class loader first delegates the class lookup to its
     * parent class loader. So once NativeLoader is loaded by the root
     * class loader, no child class loader initialize NativeLoader again.
     * 
     * @return
     */
    static synchronized Object load(String[] classesToPreload, String nativeLibName) {

        
        // TODO check if we load already className
        
        try {
            if (!hasInjectedNativeLoader()) {
                // Inject NativeLoader (/com/koda/common/nativelib/NativeLoader.bytecode) to the root class loader  
                Class< ? > nativeLoader = injectNativeLoader(classesToPreload);
                // Load the JNI code using the injected loader
                loadNativeLibrary(nativeLoader, nativeLibName);
            }
            // Look up Native - which is first in the list, injected to the root classloder, using reflection to order to avoid the initialization of Native class in this context class loader.
            return Class.forName(classesToPreload[0]).newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new KodaError(KodaErrorCode.FAILED_TO_LOAD_NATIVE_LIBRARY, e.getMessage());
        }

    }

    private static Class< ? > injectNativeLoader(String[] classesToPreload) {

        try {
            // Use parent class loader to load Native lib, since Tomcat, which uses different class loaders for each webapps, cannot load JNI interface twice

            final String nativeLoaderClassName = "com.koda.common.nativelib.NativeLoader";
            ClassLoader rootClassLoader = getRootClassLoader();
            // Load a byte code 
            byte[] byteCode = getByteCode("/com/koda/common/nativelib/NativeLoader.bytecode");
            // In addition, we need to load the other dependent classes (e.g., SnappyNative and SnappyException) using the system class loader
 
            List<byte[]> preloadClassByteCode = new ArrayList<byte[]>(classesToPreload.length);
            for (String each : classesToPreload) {
                preloadClassByteCode.add(getByteCode(String.format("/%s.class", each.replaceAll("\\.", "/"))));
            }

            // Create SnappyNativeLoader class from a byte code
            Class< ? > classLoader = Class.forName("java.lang.ClassLoader");
            Method defineClass = classLoader.getDeclaredMethod("defineClass", new Class[] { String.class, byte[].class,
                    int.class, int.class, ProtectionDomain.class });

            ProtectionDomain pd = System.class.getProtectionDomain();

            // ClassLoader.defineClass is a protected method, so we have to make it accessible
            defineClass.setAccessible(true);
            try {
                // Create a new class using a ClassLoader#defineClass
                defineClass.invoke(rootClassLoader, nativeLoaderClassName, byteCode, 0, byteCode.length, pd);

                // And also define dependent classes in the root class loader
                for (int i = 0; i < classesToPreload.length; ++i) {
                    byte[] b = preloadClassByteCode.get(i);
                    defineClass.invoke(rootClassLoader, classesToPreload[i], b, 0, b.length, pd);
                }
            }
            finally {
                // Reset the accessibility to defineClass method
                defineClass.setAccessible(false);
            }

            // Load the yNativeLoader class
            return rootClassLoader.loadClass(nativeLoaderClassName);

        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            throw new KodaError(KodaErrorCode.FAILED_TO_LOAD_NATIVE_LIBRARY, e.getMessage());
        }

    }

    /**
     * Load java's native code using load method of the
     * NativeLoader class injected to the root class loader.
     * 
     * @param loaderClass
     * @param libName - library name
     * @throws SecurityException
     * @throws NoSuchMethodException
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private static void loadNativeLibrary(Class< ? > loaderClass, String libName) throws SecurityException, NoSuchMethodException,
            IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        if (loaderClass == null)
            throw new KodaError(KodaErrorCode.FAILED_TO_LOAD_NATIVE_LIBRARY, "missing native loader class");

        File nativeLib = findNativeLibrary(libName);
        if (nativeLib != null) {
            // Load extracted or specified java native library. 
            Method loadMethod = loaderClass.getDeclaredMethod("load", new Class[] { String.class });
            loadMethod.invoke(null, nativeLib.getAbsolutePath());
        }
        else {
            // Load preinstalled lib (in the path -Djava.library.path) 
            Method loadMethod = loaderClass.getDeclaredMethod("loadLibrary", new Class[] { String.class });
            loadMethod.invoke(null, libName);
        }
    }

    public synchronized static void loadNativeLibrary(String libName) {
        if(isLoadedSet.contains(libName)) return;
        File nativeLib = findNativeLibrary(libName);
        if(nativeLib != null){
            System.load(nativeLib.getAbsolutePath());
        } else{
            System.loadLibrary(libName);
        }       
        isLoadedSet.add(libName);
    }
    
    /**
     * Computes the MD5 value of the input stream
     * 
     * @param input
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    static String md5sum(InputStream input) throws IOException {
        BufferedInputStream in = new BufferedInputStream(input);
        try {
            MessageDigest digest = java.security.MessageDigest.getInstance("MD5");
            DigestInputStream digestInputStream = new DigestInputStream(in, digest);
            for (; digestInputStream.read() >= 0;) {

            }
            ByteArrayOutputStream md5out = new ByteArrayOutputStream();
            md5out.write(digest.digest());
            return md5out.toString();
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm is not available: " + e);
        }
        finally {
            in.close();
        }
    }

    private static String mapLibraryName(String libName)
    {
        String mapped = System.mapLibraryName(libName);
        return fixIfMacOSX(mapped);
    }
    
    /**
     * Extract the specified library file to the target folder
     * 
     * @param libFolderForCurrentOS
     * @param libraryFileName
     * @param targetFolder
     * @return
     */
    private static File extractLibraryFile(String libFolderForCurrentOS, String libraryName, String targetFolder) {
        String libraryFileName = mapLibraryName(libraryName);
        String nativeLibraryFilePath = libFolderForCurrentOS + "/" + libraryFileName;
        final String prefix = "native-" + getVersion(libraryName) + "-";
        String extractedLibFileName = prefix + libraryFileName;
        File extractedLibFile = new File(targetFolder, extractedLibFileName);

        try {
            if (extractedLibFile.exists()) {

                // test md5sum value
                String md5sum1 = md5sum(LibLoader.class.getResourceAsStream(nativeLibraryFilePath));
                String md5sum2 = md5sum(new FileInputStream(extractedLibFile));

                if (md5sum1.equals(md5sum2)) {
                    return new File(targetFolder, extractedLibFileName);
                }
                else {
                    // remove old native library file
                    boolean deletionSucceeded = extractedLibFile.delete();
                    if (!deletionSucceeded) {
                        throw new IOException("failed to remove existing native library file: "
                                + extractedLibFile.getAbsolutePath());
                    }
                }
            } 

            // Extract a native library file into the target directory
            InputStream reader = LibLoader.class.getResourceAsStream(nativeLibraryFilePath);
            FileOutputStream writer = new FileOutputStream(extractedLibFile);
            byte[] buffer = new byte[8192];
            int bytesRead = 0;
            while ((bytesRead = reader.read(buffer)) != -1) {
                writer.write(buffer, 0, bytesRead);
            }

            writer.close();
            reader.close();

            // Set executable (x) flag to enable Java to load the native library
            if (!System.getProperty("os.name").contains("Windows")) {
                try {
                    Runtime.getRuntime().exec(new String[] { "chmod", "755", extractedLibFile.getAbsolutePath() })
                            .waitFor();
                }
                catch (Throwable e) {}
            }

            return new File(targetFolder, extractedLibFileName);
        }
        catch (IOException e) {
            e.printStackTrace(System.err);
            return null;
        }
    }

    static File findNativeLibrary(String libName) {

        boolean useSystemLib = Boolean.parseBoolean(System.getProperty(KEY_KODA_USE_SYSTEMLIB, "false"));
        if (useSystemLib)
            return null;

        boolean disabledBundledLibs = Boolean.parseBoolean(System.getProperty(KEY_KODA_DISABLE_BUNDLED_LIBS, "false"));
        if (disabledBundledLibs)
            return null;

        // Try to load the library in com.koda.lib.path */
        String nativeLibraryPath = System.getProperty(KEY_KODA_LIB_PATH);
        // Resolve the library file name with a suffix (e.g., dll, .so, etc.)
        String nativeLibraryName = mapLibraryName(libName);

        nativeLibraryName = fixIfMacOSX(nativeLibraryName);
        
        if (nativeLibraryPath != null) {
            File nativeLib = new File(nativeLibraryPath, nativeLibraryName);
            if (nativeLib.exists())
                return nativeLib;
        }

        // Load an OS-dependent native library inside a jar file
        // We have standard dircetory structure for JNI lib
        // 1. /com/koda/<libname>/native - where alll JNI libs are stored
        // 2. /com/koda/<libname>/VERSION - file which contains version name
        nativeLibraryPath = "/com/koda/" + libName + "/native/" + OSInfo.getNativeLibFolderPathForCurrentOS();

        if (LibLoader.class.getResource(nativeLibraryPath + "/" + nativeLibraryName) != null) {
            // Temporary library folder. Use the value of com.koda.tempdir or java.io.tmpdir
            String tempFolder = new File(System.getProperty(KEY_KODA_TEMPDIR, System.getProperty("java.io.tmpdir")))
                            .getAbsolutePath();

            // Extract and load a native library inside the jar file
            return extractLibraryFile(nativeLibraryPath, libName, tempFolder);
        }

        return null; // Use a pre-installed ?
    }

    private static String fixIfMacOSX(String nativeLibraryName) {
        
        if(OSInfo.getOSName() == "Mac"){
            int index = nativeLibraryName.lastIndexOf('.');
            return nativeLibraryName.substring(0, index) +".dylib";
        }
        return nativeLibraryName;
    }

    public static String getVersion(String libName) {

        URL versionFile = LibLoader.class.getResource("/com/koda/"+libName+"/VERSION");

        String version = "unknown";
        try {
            if (versionFile != null) {
                Properties versionData = new Properties();
                versionData.load(versionFile.openStream());
                version = versionData.getProperty("version", version);
                if (version.equals("unknown"))
                    version = versionData.getProperty("VERSION", version);
                version = version.trim().replaceAll("[^0-9\\.]", "");
            }
        }
        catch (IOException e) {
            System.err.println(e);
        }
        return version;
    }

}

