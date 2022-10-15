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
package com.dremio.provision.yarn;

import static com.google.common.base.StandardSystemProperty.JAVA_CLASS_PATH;
import static com.google.common.base.StandardSystemProperty.PATH_SEPARATOR;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.validation.constraints.NotNull;

import com.dremio.config.DremioConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.MustBeClosed;

/**
 * An application bundle generator to be used with {@code DremioTwillRunner}
 *
 * The generated bundle is created from the provided classloader: the classloader
 * (and its parents) is examined to extract URLs used to set it up. Each URL is
 * then copied over to the bundle as-is (recursively if the URL targets a directory).
 * The same is done with native libraries.
 *
 * Once all files are copied over, a manifest file is added to the jar containing
 * the ordered list of jars and native libraries so that the runner can reconstruct
 * the same classpath remotely.
 */
public class AppBundleGenerator {

  public static final String X_DREMIO_LIBRARY_PATH_MANIFEST_ATTRIBUTE = "X-Dremio-Library-Path";
  public static final String X_DREMIO_PLUGINS_PATH_MANIFEST_ATTRIBUTE = "X-Dremio-Plugins-Path";
  private static final String DREMIO_BUNDLE_PREFIX = "dremio-bundle";
  private static final Path DREMIO_APP_PATH = Paths.get("dremio.app");
  private static final String DELIMITER = " ";

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AppBundleGenerator.class);

  private final ClassLoader classLoader;
  private final List<String> classPathPrefix;
  private final List<String> classPath;
  private final List<String> nativeLibraryPath;
  private final Path pluginsPath;

  // A simple wrapper to create a Jar file
  static interface JarGenerator extends Closeable {
    void addDirectory(String name) throws IOException;

    void addFile(String name, Path source) throws IOException;

    void addManifest(Manifest manifest) throws IOException;

    static JarGenerator of(final JarOutputStream jos) {
      final Set<String> added = new HashSet<>();

      return new JarGenerator() {

        @Override
        public void close() throws IOException {
          jos.close();
        }

        @Override
        public void addFile(String name, Path source) throws IOException {
          if (!added.add(name)) {
            // File has already been added under the same name
            logger.warn("Duplicate entry for {} (with source {}). Only first entry is preserved.", name, source);
            return;
          }

          jos.putNextEntry(new JarEntry(name));
          try {
            Files.copy(source, jos);
          } finally {
            jos.closeEntry();
          }
        }

        @Override
        public void addDirectory(String name) throws IOException {
          Preconditions.checkArgument(name.endsWith("/"));

          if (!added.add(name)) {
            // Directory has already been added under the same name
            logger.warn("Duplicate entry for {}. Only first entry is preserved.", name);
            return;
          }

          jos.putNextEntry(new JarEntry(name));
          jos.closeEntry();

        }

        @Override
        public void addManifest(Manifest manifest) throws IOException {
          if (!added.add(JarFile.MANIFEST_NAME)) {
            // Manifest has already been added under the same name
            logger.warn("Duplicate manifest entry for {}. Only first entry is preserved.", JarFile.MANIFEST_NAME);
            return;
          }

          jos.putNextEntry(new JarEntry(JarFile.MANIFEST_NAME));
          try {
            manifest.write(jos);
          } finally {
            jos.closeEntry();
          }
        }
      };
    }
  }

  public AppBundleGenerator(ClassLoader classLoader,
                            @NotNull List<String> classPathPrefix,
                            @NotNull List<String> classPath,
                            List<String> nativeLibraryPath,
                            Path pluginsPath) {
    this.classLoader = classLoader;
    this.classPathPrefix = ImmutableList.copyOf(classPathPrefix);
    this.classPath = ImmutableList.copyOf(classPath);
    this.nativeLibraryPath = ImmutableList.copyOf(nativeLibraryPath);
    this.pluginsPath = pluginsPath;
  }

  public static AppBundleGenerator of(DremioConfig config) {

    List<String> nativeLibraryPath = Optional.ofNullable(StandardSystemProperty.JAVA_LIBRARY_PATH.value())
        .map(p -> Arrays.asList(p.split(File.pathSeparator)))
        .orElse(Collections.emptyList());

    final Path pluginsPath = DremioConfig.getPluginsRootPath();

    return new AppBundleGenerator(
        AppBundleGenerator.class.getClassLoader(),
        config.getStringList(DremioConfig.YARN_APP_CLASSPATH_PREFIX),
        config.getStringList(DremioConfig.YARN_APP_CLASSPATH),
        nativeLibraryPath,
        pluginsPath
    );
  }

  public Path generateBundle() throws IOException {
    // Create an application bundle jar based on current application classpath
    Path yarnBundledJarPath = Files.createTempFile(DREMIO_BUNDLE_PREFIX, ".jar");
    try (JarGenerator jarGenerator = JarGenerator.of(new JarOutputStream(Files.newOutputStream(yarnBundledJarPath)))) {
      // First add prefix classpath entries
      // Second, add content of classpath to bundle jar
      // Then add extra classpath entries
      List<URI> jarEntries;
      try (Stream<Path> prefixStream = toPathStream(classPathPrefix);
        Stream<Path> classLoaderStream = toPathStream(classLoader);
        Stream<Path> classPathStream = toPathStream(classPath)) {
        jarEntries = addPathsToBundle(jarGenerator,
          Stream.concat(prefixStream, Stream.concat(classLoaderStream, classPathStream)));
      }

      // After that add native libraries
      List<URI> nativeLibrariesEntries = addPathsToBundle(jarGenerator, nativeLibraryPath.stream().map(Paths::get));

      // After that add plugins
      URI pluginsPathEntry = addPathToJar(jarGenerator, pluginsPath);

      // Finally, add classpath and native library path entries in jar manifest
      // Following spec for class-path, string is a list of URI separated by space...
      Manifest manifest = new Manifest();
      final Attributes mainAttributes = manifest.getMainAttributes();
      mainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0");
      mainAttributes.put(Attributes.Name.CLASS_PATH,
          jarEntries.stream()
          .map(URI::toString)
          .collect(Collectors.joining(DELIMITER)));
      mainAttributes.putValue(X_DREMIO_LIBRARY_PATH_MANIFEST_ATTRIBUTE,
          nativeLibrariesEntries.stream().map(URI::toString).collect(Collectors.joining(DELIMITER)));
      mainAttributes.putValue(X_DREMIO_PLUGINS_PATH_MANIFEST_ATTRIBUTE, pluginsPathEntry.toString());

      jarGenerator.addManifest(manifest);
    }

    return yarnBundledJarPath;
  }

  @VisibleForTesting
  static Stream<Path> toPathStream(ClassLoader classLoader) {
    if (classLoader == null) {
      return Stream.of();
    }

    if (classLoader != ClassLoader.getSystemClassLoader()) {
      // First add jar/classes loaded by the parent classloader in order to preserve
      // ordering.
      // But, don't go past the system classloader as JVM classes/jars should not be copied
      // over!
      return Stream.concat(toPathStream(classLoader.getParent()), getPaths(classLoader));
    } else {
      return getPaths(classLoader);
    }
  }

  private static Stream<Path> parseJavaClassPath() {
    ImmutableList.Builder<Path> paths = ImmutableList.builder();
    for (String entry : Splitter.on(PATH_SEPARATOR.value()).split(JAVA_CLASS_PATH.value())) {
      paths.add(Paths.get(entry));
    }
    return paths.build().stream();
  }

  private static Stream<Path> getPaths(ClassLoader classLoader) {
    if (!(classLoader instanceof URLClassLoader)) {
      // in Jdk8 the default classloader is of type URLClassLoader
      // in Jdk9+ the default classloader is NOT of URLClassloader.
      // SystemClassLoader loads classes from the classpath.
      // Hence resolving all the resources from the classpath will include
      // the required dependencies.
      if (Objects.equals(classLoader, ClassLoader.getSystemClassLoader())) {
        return parseJavaClassPath();
      } else {
        return Stream.of();
      }
    }

    URLClassLoader urlCL = (URLClassLoader) classLoader;

    return Stream.of(urlCL.getURLs())
        .flatMap(url -> {
          try {
            return Stream.of(url.toURI());
          } catch (URISyntaxException e) {
            logger.warn("Invalid URI `{}` detected while analyzing classpath", url, e);
            return Stream.empty();
          }
        })
        .filter(uri -> {
          if (!"file".equals(uri.getScheme())) {
            logger.warn("Non-file URI `{}` detected while analyzing classpath, ignoring.", uri);
            return false;
          }
          return true;
        })
        .map(Paths::get);
  }

  @SuppressWarnings("StreamResourceLeak")
  @MustBeClosed
  @VisibleForTesting
  static Stream<Path> toPathStream(List<String> classpathJars) {
    return classpathJars.stream().flatMap(classpathJar -> {
      final Path jarFullPath = Paths.get(classpathJar);
      final Path parentDir = jarFullPath.getParent();
      final String fileName = jarFullPath.getFileName().toString();

      // do not close the stream!
      try {
        return Files.list(parentDir).filter(p -> p.getFileName().toString().matches(fileName));
      } catch (IOException e) {
        logger.error("Not able to list path {} content", parentDir, e);
        return Stream.of();
      }
    });
  }

  private List<URI> addPathsToBundle(JarGenerator jarGenerator, Stream<Path> stream) {
        return stream
        .map(path -> addPathToJar(jarGenerator, path))
        .collect(Collectors.toList());
  }

  private List<URI> addJavaLibraryPathToBundle(JarGenerator jarGenerator) throws IOException {
    String javaLibraryPath = StandardSystemProperty.JAVA_LIBRARY_PATH.value();
    if (javaLibraryPath == null) {
      return Collections.emptyList();
    }

    return addPathsToBundle(jarGenerator, Stream.of(javaLibraryPath.split(File.pathSeparator)).map(Paths::get));
  }

  /**
   * Add a path to the bundle, recursively if the path denotes a directory
   */
  private URI addPathToJar(JarGenerator jarGenerator, Path path) {
    URI entryURI = doAddPathToJar(jarGenerator, path);

    if (Files.isDirectory(path)) {
      // List the content of the directory and add it as-is
      // Make sure to check if file was not already added (in case a subtree was added to the
      // classpath too)
      try(Stream<Path> list = Files.list(path)) {
        list.forEach(subPath -> {
          // Ignore return value, directory is being added
          addPathToJar(jarGenerator, subPath);
        });
      } catch (IOException e) {
        logger.error("Not able to list path {} content", path, e);
      }
    }

    return entryURI;
  }

  /**
   * Add a single file to the bundle and return the URI representing the jar entry
   */
  private URI doAddPathToJar(JarGenerator jarGenerator, Path path) {
    final Path entryPath = DREMIO_APP_PATH.resolve(path.subpath(0, path.getNameCount()));
    final URI result;
    // Cannot use Path#toUri: need to return a relative URI
    try {
      result = new URI(null, null, entryPath.toString(), null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }

    if (!Files.exists(path)) {
      logger.debug("File `{}` does not exist, skipping", path.toUri());
      return result;
    }

    try {
      if (Files.isDirectory(path)) {
        jarGenerator.addDirectory(entryPath.toString() + "/");
      } else {
        jarGenerator.addFile(entryPath.toString(), path);
      }

    } catch (IOException e) {
      logger.error("Not able to add file {} to Dremio YARN bundle jar", path, e);
    }

    return result;
  }
}
