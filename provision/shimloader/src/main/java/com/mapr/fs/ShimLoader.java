/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mapr.fs;

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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.Manifest;

/**
 * ShimLoader
 *
 * <p>Shimloader replacement which does not look for native libraries in system-wide locations
 */
// CHECKSTYLE:OFF FinalClass
public class ShimLoader {
  static final String NATIVE_LOADER_CLASS_NAME = "com.mapr.fs.shim.LibraryLoader";
  static final String[] PRELOAD_CLASSES =
      new String[] {
        "com.mapr.fs.jni.Errno",
        "com.mapr.fs.jni.MapRConstants",
        "com.mapr.fs.jni.MapRConstants$JniUsername",
        "com.mapr.fs.jni.MapRConstants$ErrorValue",
        "com.mapr.fs.jni.MapRConstants$RowConstants",
        "com.mapr.fs.jni.MapRConstants$PutConstants",
        "com.mapr.fs.jni.JNIBlockLocation",
        "com.mapr.fs.jni.JNIFsStatus",
        "com.mapr.fs.jni.JNIFileStatus",
        "com.mapr.fs.jni.JNIFileStatus$VolumeInfo",
        "com.mapr.fs.jni.JNILoggerProxy",
        "com.mapr.fs.jni.IPPort",
        "com.mapr.fs.jni.GatewaySource",
        "com.mapr.fs.jni.Page",
        "com.mapr.fs.jni.Page$CacheState",
        "com.mapr.fs.jni.InodeAttributes",
        "com.mapr.fs.jni.SFid",
        "com.mapr.fs.jni.MapRAsyncRpc",
        "com.mapr.fs.jni.MapRGet",
        "com.mapr.fs.jni.MapRJSONPut",
        "com.mapr.fs.jni.MapRPut",
        "com.mapr.fs.jni.MapRIncrement",
        "com.mapr.fs.jni.MapRKeyValue",
        "com.mapr.fs.jni.MapRRowConstraint",
        "com.mapr.fs.jni.MapRScan",
        "com.mapr.fs.jni.MapRCallBackQueue",
        "com.mapr.fs.jni.MapRClient",
        "com.mapr.fs.jni.MapRTableTools",
        "com.mapr.security.JNISecurity",
        "com.mapr.security.JNISecurity$MutableErr",
        "com.mapr.security.UnixUserGroupHelper",
        "com.mapr.fs.jni.MapRUserGroupInfo",
        "com.mapr.fs.jni.MapRUserInfo",
        "com.mapr.fs.jni.RpcNative",
        "com.mapr.fs.RpcCallContext",
        "com.mapr.fs.jni.MapRClientInitParams",
        "com.mapr.fs.jni.RowColDecoder",
        "com.mapr.fs.jni.RowColDecoder$1",
        "com.mapr.fs.jni.RowColDecoderCallback",
        "com.mapr.fs.jni.RowColParser",
        "com.mapr.fs.jni.RowColParser$1",
        "com.mapr.fs.jni.RowColParser$STATE",
        "com.mapr.fs.jni.RowColParser$ValType",
        "com.mapr.fs.jni.MapRResult",
        "com.mapr.fs.jni.MapRResult$MapRResultDecoderCallback",
        "com.mapr.fs.jni.ParsedRow",
        "com.mapr.fs.jni.MarlinProducerResult",
        "com.mapr.fs.jni.NativeData",
        "com.mapr.fs.jni.MarlinJniClient",
        "com.mapr.fs.jni.MarlinJniAdmin",
        "com.mapr.fs.jni.MarlinJniProducer",
        "com.mapr.fs.jni.MarlinJniListener"
      };
  static final String[] WEBAPP_SYSTEM_CLASSES = new String[] {"com.mapr.fs.jni."};
  private static volatile boolean isLoaded = false;
  private static boolean debugLog = System.getProperty("shimloader.debuglog") != null;
  private static final String USER_NAME =
      System.getProperty("user.name").replaceAll("[\\\\/:]", "_");
  private static final String LIBRARY_VERSION = getLibraryVersion(ShimLoader.class);

  private ShimLoader() {}

  private static ClassLoader getRootClassLoader() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = ShimLoader.class.getClassLoader();
    }

    trace("getRootClassLoader: thread classLoader is '%s'", cl.getClass().getCanonicalName());

    while (cl.getParent() != null) {
      cl = cl.getParent();
    }

    trace("getRootClassLoader: root classLoader is '%s'", cl.getClass().getCanonicalName());
    return cl;
  }

  private static byte[] getByteCode(String resourcePath) throws IOException {
    InputStream in = ShimLoader.class.getResourceAsStream(resourcePath);
    if (in == null) {
      throw new IOException(resourcePath + " is not found");
    } else {
      byte[] buf = new byte[1024];
      ByteArrayOutputStream byteCodeBuf = new ByteArrayOutputStream();

      int readLength;
      while ((readLength = in.read(buf)) != -1) {
        byteCodeBuf.write(buf, 0, readLength);
      }

      in.close();
      return byteCodeBuf.toByteArray();
    }
  }

  public static boolean isNativeLibraryLoaded() {
    return isLoaded;
  }

  private static boolean isMaprClntLibLoaded() {
    boolean loaderLoaded = false;

    try {
      Class<?> loaderClass = Class.forName("com.mapr.fs.shim.LibraryLoader");
      Method getMethod = loaderClass.getDeclaredMethod("isMapRClntLibLoaded");
      loaderLoaded = (Boolean) getMethod.invoke((Object) null);
    } catch (Exception ex) {
      loaderLoaded = false;
    }

    return loaderLoaded;
  }

  public static synchronized void load() {
    if (isLoaded) {
      trace("MapR native classes already loaded");
    } else {
      boolean loadInRootClassloader = System.getProperty("mapr.library.flatclass") == null;
      trace("Load in root Classloader: %s.", loadInRootClassloader);

      try {
        if (loadInRootClassloader) {
          if (!isMaprClntLibLoaded()) {
            trace("Injecting Native Loader");
            String synchronizationString = "com.mapr.fs.shim.LibraryLoader".intern();
            synchronized (synchronizationString) {
              if (!isMaprClntLibLoaded()) {
                Class<?> nativeLoader = injectNativeLoader();
                loadNativeLibrary(nativeLoader);
                trace("Native Loader injected");
              }
            }
          }

          addSystemClassesToWebApps(PRELOAD_CLASSES);
        } else {
          loadNativeLibrary(System.class);
        }

        isLoaded = true;
      } catch (Exception ex) {
        trace("Unable to load libMapRClient.so native library.");
        ex.printStackTrace(System.err);
        throw new ExceptionInInitializerError(ex);
      }
    }
  }

  private static Class<?> injectNativeLoader() {
    try {
      ClassLoader rootClassLoader = getRootClassLoader();
      byte[] libLoaderByteCode = getByteCode("/com/mapr/fs/shim/LibraryLoader.bytecode");
      List<byte[]> preloadClassByteCode = new ArrayList(PRELOAD_CLASSES.length);
      String[] preloadClasses = PRELOAD_CLASSES;
      int size = preloadClasses.length;

      for (int j = 0; j < size; ++j) {
        String each = preloadClasses[j];
        preloadClassByteCode.add(
            getByteCode(String.format("/%s.class", each.replaceAll("\\.", "/"))));
      }

      Class<?> classLoader = Class.forName("java.lang.ClassLoader");
      Method defineClass =
          classLoader.getDeclaredMethod(
              "defineClass",
              String.class,
              byte[].class,
              Integer.TYPE,
              Integer.TYPE,
              ProtectionDomain.class);
      ProtectionDomain pd = System.class.getProtectionDomain();
      defineClass.setAccessible(true);

      try {
        trace("injectNativeLoader: Loading MapR native classes");
        defineClass.invoke(
            rootClassLoader,
            "com.mapr.fs.shim.LibraryLoader",
            libLoaderByteCode,
            0,
            libLoaderByteCode.length,
            pd);

        for (int i = 0; i < PRELOAD_CLASSES.length; ++i) {
          byte[] b = (byte[]) preloadClassByteCode.get(i);
          defineClass.invoke(rootClassLoader, PRELOAD_CLASSES[i], b, 0, b.length, pd);
        }
      } catch (InvocationTargetException invocationTargetException) {
        throw invocationTargetException;
      } finally {
        defineClass.setAccessible(false);
      }

      return rootClassLoader.loadClass("com.mapr.fs.shim.LibraryLoader");
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw new RuntimeException("Failure loading MapRClient. ", ex);
    }
  }

  private static void loadNativeLibrary(Class<?> loaderClass) throws Exception {
    // DX-16226: Unlike the original MapR-FS ShimLoader, Dremio's ShimLoader only look for bundled
    // native libraries
    if (loaderClass == null) {
      throw new RuntimeException("Missing LibraryLoader native loader class");
    } else {
      try {
        File nativeLib = findNativeLibrary();
        if (nativeLib != null) {
          Method loadMethod;
          loadMethod = loaderClass.getDeclaredMethod("load", String.class);
          loadMethod.invoke((Object) null, nativeLib.getAbsolutePath());
          trace("Native library loaded.");
          if ("com.mapr.fs.shim.LibraryLoader".equals(loaderClass.getName())) {
            Method setMethod = loaderClass.getDeclaredMethod("setMaprClntLibLoaded");
            setMethod.invoke((Object) null);
          }
        } else {
          throw new Exception("unable to load NativeLibrary");
        }
      } catch (RuntimeException runtimeException) {
        System.err.println(
            "==========Unable to find library on native path due to Exception. ==============");
        runtimeException.printStackTrace(System.err);
        System.err.println(
            "==========Unable to find library in jar due to exception. ==============");
        runtimeException.printStackTrace(System.err);
        throw runtimeException;
      }
    }
  }

  static String md5sum(InputStream input) throws IOException {
    BufferedInputStream in = new BufferedInputStream(input);

    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      DigestInputStream digestInputStream = new DigestInputStream(in, digest);
      boolean bytesRead = false;
      byte[] buffer = new byte[8192];

      while (digestInputStream.read(buffer) != -1) {
        // read until end
      }

      ByteArrayOutputStream md5out = new ByteArrayOutputStream();
      md5out.write(digest.digest());
      String md5String = md5out.toString();
      return md5String;
    } catch (NoSuchAlgorithmException noSuchAlgorithmException) {
      throw new IllegalStateException(
          "MD5 algorithm is not available: " + noSuchAlgorithmException);
    } finally {
      in.close();
    }
  }

  private static File extractLibraryFile(
      String libFolderForCurrentOS, String libraryFileName, String targetFolder) {
    trace("Extracting native library to '%s'.", targetFolder);
    int extentionStart = libraryFileName.lastIndexOf(46);
    String extractedLibFileName =
        "mapr-"
            + USER_NAME
            + "-"
            + libraryFileName.substring(0, extentionStart + 1)
            + LIBRARY_VERSION
            + libraryFileName.substring(extentionStart);
    File extractedLibFile = new File(targetFolder, extractedLibFileName);
    trace("Native library for this platform is '%s'.", extractedLibFileName);

    try {
      String nativeLibraryFilePath = libFolderForCurrentOS + "/" + libraryFileName;
      if (extractedLibFile.exists()) {
        trace(
            "Target file '%s' already exists, verifying checksum.",
            extractedLibFile.getAbsolutePath());
        String md5sum1 = md5sum(ShimLoader.class.getResourceAsStream(nativeLibraryFilePath));
        String md5sum2 = md5sum(new FileInputStream(extractedLibFile));
        if (md5sum1.equals(md5sum2)) {
          trace("Checksum matches, will not extract from the JAR.");
          return extractedLibFile;
        }

        trace("Checksum did not match, will replace existing file from the JAR.");
        if (!extractedLibFile.delete()) {
          throw new IOException(
              "Failed to remove existing native library file: "
                  + extractedLibFile.getAbsolutePath());
        }
      }

      trace("Target file '%s' does not exist, will extract from the JAR.", extractedLibFile);
      InputStream reader = ShimLoader.class.getResourceAsStream(nativeLibraryFilePath);
      File targetFolderFile = new File(targetFolder);
      if (!targetFolderFile.exists()) {
        trace("Creating target folder %s", targetFolder);
        targetFolderFile.mkdirs();
      }

      FileOutputStream writer = new FileOutputStream(extractedLibFile);
      byte[] buffer = new byte[8192];
      boolean b = false;

      int bytesRead;
      while ((bytesRead = reader.read(buffer)) != -1) {
        writer.write(buffer, 0, bytesRead);
      }

      writer.close();
      reader.close();
      if (!System.getProperty("os.name").contains("Windows")) {
        try {
          Runtime.getRuntime()
              .exec(new String[] {"chmod", "755", extractedLibFile.getAbsolutePath()})
              .waitFor();
        } catch (Throwable throwable) {
          trace("Error setting executable permission.\n%s.", throwable.getMessage());
        }
      }

      return extractedLibFile;
    } catch (IOException ex) {
      ex.printStackTrace(System.err);
      return null;
    }
  }

  public static String getLibraryVersion(Class<?> clazz) {
    String libVersion = "unknown";

    try {
      String className = clazz.getSimpleName() + ".class";
      String qualifiedClassName = clazz.getName().replace('.', '/') + ".class";
      String classURL = clazz.getResource(className).toString();
      int endIndex =
          classURL.startsWith("jar:")
              ? classURL.lastIndexOf("!") + 1
              : classURL.lastIndexOf(qualifiedClassName) - 1;
      String manifestPath = classURL.substring(0, endIndex) + "/META-INF/MANIFEST.MF";
      Manifest manifest = new Manifest((new URL(manifestPath)).openStream());
      Attributes attr = manifest.getMainAttributes();
      Name attrName = new Name("Implementation-Version");
      if (attr.containsKey(attrName)) {
        libVersion = attr.getValue(attrName);
      } else {
        attrName = new Name("Bundle-Version");
        if (attr.containsKey(attrName)) {
          libVersion = attr.getValue(attrName);
        }
      }
    } catch (Throwable throwable) {

    }

    return libVersion;
  }

  static File findNativeLibrary() {
    String nativeLibraryName = System.mapLibraryName("MapRClient");
    String nativeLibraryPath = "/com/mapr/fs/native/" + OSInfo.getNativeLibFolderPathForCurrentOS();
    trace("Searching for native library '%s/%s'.", nativeLibraryPath, nativeLibraryName);
    boolean hasNativeLib = hasResource(nativeLibraryPath + "/" + nativeLibraryName);
    String tempFolder;
    if (!hasNativeLib && OSInfo.getOSName().equals("Mac")) {
      tempFolder = "libMapRClient.dylib";
      trace("Searching for alternative library '%s' on Mac.", tempFolder);
      if (hasResource(nativeLibraryPath + "/" + tempFolder)) {
        nativeLibraryName = tempFolder;
        hasNativeLib = true;
      }
    }

    if (!hasNativeLib) {
      tempFolder =
          String.format(
              "no native library is found for os.name=%s and os.arch=%s",
              OSInfo.getOSName(), OSInfo.getArchName());
      trace(tempFolder);
      throw new RuntimeException(tempFolder);
    } else {
      tempFolder = (new File(System.getProperty("java.io.tmpdir"))).getAbsolutePath();
      return extractLibraryFile(nativeLibraryPath, nativeLibraryName, tempFolder);
    }
  }

  private static boolean hasResource(String path) {
    return ShimLoader.class.getResource(path) != null;
  }

  private static void addSystemClassesToWebApps(String[] systemClasses) {
    try {
      Class jettyWebAppContextClass = null;

      try {
        jettyWebAppContextClass = Class.forName("org.eclipse.jetty.webapp.WebAppContext");
      } catch (ClassNotFoundException classNotFoundException) {
        jettyWebAppContextClass = Class.forName("org.mortbay.jetty.webapp.WebAppContext");
      }

      Method getCurrentWebAppContextMethod =
          jettyWebAppContextClass.getMethod("getCurrentWebAppContext");
      Method getSystemClassesMethod = jettyWebAppContextClass.getMethod("getSystemClasses");
      Method setSystemClassesMethod =
          jettyWebAppContextClass.getMethod("setSystemClasses", String[].class);
      Object jettyCurrentWebAppContext = getCurrentWebAppContextMethod.invoke((Object) null);
      if (jettyCurrentWebAppContext != null) {
        String[] currentSystemClasses =
            (String[]) ((String[]) getSystemClassesMethod.invoke(jettyCurrentWebAppContext));
        List<String> newSystemClasses = new ArrayList();
        Collections.addAll(newSystemClasses, currentSystemClasses);
        Collections.addAll(newSystemClasses, systemClasses);
        Object[] newSystemClassesAsObjectArray =
            new Object[] {newSystemClasses.toArray(new String[0])};
        setSystemClassesMethod.invoke(jettyCurrentWebAppContext, newSystemClassesAsObjectArray);
      }
    } catch (ClassNotFoundException classNotFoundException) {
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  static void trace(String msg, Object... args) {
    if (debugLog) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      System.err.println(
          dateFormat.format(new Date())
              + " ["
              + Thread.currentThread().getId()
              + "] "
              + String.format(msg, args));
    }
  }

  public static void main(String[] args) {
    debugLog = true;
    trace("ShimLoader library version: %s.", LIBRARY_VERSION);
    if (args.length > 0 && args[0].equals("load")) {
      load();
    } else {
      trace("Native library path: '%s'.", findNativeLibrary());
    }
  }
}
// CHECKSTYLE:ON
