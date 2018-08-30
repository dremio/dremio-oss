/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hamcrest.CoreMatchers;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;


/**
 * Test {@code AppBundleGenerator} class
 */
public class TestAppBundleGenerator {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testToPathStream() throws MalformedURLException, IOException {
    try (
        URLClassLoader clA = new URLClassLoader(
            new URL[] { new URL("file:/foo/bar.jar"), new URL("file:/foo/baz.jar") }, null);
        URLClassLoader clB = new URLClassLoader(
            new URL[] { new URL("file:/test/ab.jar"), new URL("file:/test/cd.jar") }, clA)) {

      Stream<Path> stream = AppBundleGenerator.toPathStream(clB);

      assertThat(stream.collect(Collectors.toList()), is(equalTo(Arrays.asList(Paths.get("/foo/bar.jar"),
          Paths.get("/foo/baz.jar"), Paths.get("/test/ab.jar"), Paths.get("/test/cd.jar")))));
    }
  }

  @Test
  public void testSkipJDKClassLoaders() throws MalformedURLException, IOException {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    ClassLoader parent = classLoader.getParent();

    // This test is only meaningful if parent classloader exists (most likely the extension classloader
    // and is also a URLClassLoader
    Assume.assumeNotNull(parent);
    Assume.assumeTrue(parent instanceof URLClassLoader);

    URL[] parentURLs = ((URLClassLoader) parent).getURLs();

    Stream<Path> stream = AppBundleGenerator.toPathStream(classLoader);

    assertFalse("Stream contains a jar from the parent classloader", stream.anyMatch(path -> {
      try {
        return Arrays.asList(parentURLs).contains(path.toUri().toURL());
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e);
      }
    }));
  }

  @Test
  public void testToPathStreamFromClassPath() throws MalformedURLException, IOException {
    Path mainFolder = temporaryFolder.newFolder().toPath();

    Files.createFile(mainFolder.resolve("a.jar"));
    Files.createFile(mainFolder.resolve("b.jar"));
    Files.createFile(mainFolder.resolve("xyz-dremio.jar"));
    Files.createDirectory(mainFolder.resolve("www-dremio"));
    Files.createFile(mainFolder.resolve("www-dremio/foo.jar"));

    assertThat(
        AppBundleGenerator
            .toPathStream(
                Arrays.asList(mainFolder.resolve("a.jar").toString(), mainFolder.resolve(".*-dremio").toString()))
            .collect(Collectors.toList()),
        is(equalTo(Arrays.asList(mainFolder.resolve("a.jar"), mainFolder.resolve("www-dremio")))));
  }

  @Test
  public void testJarBundle() throws IOException {
    // Create several files under a temporary folder
    Path mainFolder = temporaryFolder.newFolder().toPath();
    Path outsideFolder = temporaryFolder.newFolder().toPath();

    Files.write(outsideFolder.resolve("linktofoo.jar"), Arrays.asList("some random content"), UTF_8);
    Files.write(mainFolder.resolve("bar.jar"), Arrays.asList("different random content"), UTF_8);
    Files.createSymbolicLink(mainFolder.resolve("foo.jar"), outsideFolder.resolve("linktofoo.jar"));
    Files.createDirectory(mainFolder.resolve("dir"));
    Files.write(mainFolder.resolve("dir/a.class"), Arrays.asList("random stuff"), UTF_8);
    Files.write(mainFolder.resolve("dir/b.class"), Arrays.asList("more random stuff"), UTF_8);
    Files.write(mainFolder.resolve("prefix.jar"), Arrays.asList("extra random stuff"), UTF_8);
    Files.write(mainFolder.resolve("suffix.jar"), Arrays.asList("more extra random stuff"), UTF_8);
    Files.createDirectory(mainFolder.resolve("lib"));
    Files.write(mainFolder.resolve("lib/a.so"), Arrays.asList("some fake stuff"), UTF_8);
    Files.write(mainFolder.resolve("lib/b.so"), Arrays.asList("more fake stuff"), UTF_8);

    final Path jarPath;
    try(
        URLClassLoader classLoader = new URLClassLoader(new URL[] {
            mainFolder.resolve("foo.jar").toUri().toURL(),
            mainFolder.resolve("bar.jar").toUri().toURL(),
            mainFolder.resolve("dir").toUri().toURL(),
            }, null)) {
      AppBundleGenerator generator = new AppBundleGenerator(
          classLoader,
          ImmutableList.of(mainFolder.resolve("prefix.jar").toString()),
          ImmutableList.of(mainFolder.resolve("suffix.jar").toString()),
          ImmutableList.of(mainFolder.resolve("lib").toString())
          );

      jarPath = generator.generateBundle();
    }

    try(JarFile jarFile = new JarFile(jarPath.toFile())) {
      // verify manifest
      Manifest mf = jarFile.getManifest();
      assertThat(mf, is(CoreMatchers.notNullValue()));
      assertThat(mf.getMainAttributes().get(Attributes.Name.CLASS_PATH),
          is(Arrays.asList("prefix.jar", "foo.jar", "bar.jar", "dir", "suffix.jar").stream()
              .map(s -> "dremio.app".concat(mainFolder.resolve(s).toAbsolutePath().toString()))
              .collect(Collectors.joining(" "))));
      assertThat(mf.getMainAttributes().getValue(AppBundleGenerator.X_DREMIO_LIBRARY_PATH_MANIFEST_ATTRIBUTE),
          is(Arrays.asList("lib").stream()
              .map(s -> "dremio.app".concat(mainFolder.resolve(s).toAbsolutePath().toString()))
              .collect(Collectors.joining(" "))));

      // verify content
      ImmutableMap<String, String> content = ImmutableMap.<String, String> builder()
          .put("foo.jar", "some random content\n")
          .put("bar.jar", "different random content\n")
          .put("dir/a.class", "random stuff\n")
          .put("dir/b.class", "more random stuff\n")
          .put("prefix.jar", "extra random stuff\n")
          .put("suffix.jar", "more extra random stuff\n")
          .put("lib/a.so", "some fake stuff\n")
          .put("lib/b.so", "more fake stuff\n")
          .build();

      for(Map.Entry<String, String> entry: content.entrySet()) {
        assertThat(format("Invalid content for %s", entry.getKey()),
            ByteStreams.toByteArray(
            jarFile.getInputStream(new JarEntry("dremio.app".concat(mainFolder.resolve(entry.getKey()).toAbsolutePath().toString())))),
            is(entry.getValue().getBytes(UTF_8)));
      }
    }

  }



}
