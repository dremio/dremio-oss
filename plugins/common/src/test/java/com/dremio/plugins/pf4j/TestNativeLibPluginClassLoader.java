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
package com.dremio.plugins.pf4j;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests to prevent relative path file injection from Zip Files.
 */
public class TestNativeLibPluginClassLoader {
  private static Path mockTempDir;
  private static Path mockNestedTempDir;
  private static Path mockTempDirWithTrailingSlash;
  private static Path mockTempDirInPrivateMappedDir;
  private static Path mockTempDirInPrivateMappedDirWithTrailingSlash;

  @BeforeClass
  public static void init() {
    mockTempDir = Paths.get("/tmpDir");
    mockNestedTempDir = Paths.get("/baseTmpDir/nestedTmpDir");
    mockTempDirWithTrailingSlash = Paths.get("/tmpDir/");
    mockTempDirInPrivateMappedDir = Paths.get("/var");
    mockTempDirInPrivateMappedDirWithTrailingSlash = Paths.get("/var/");
  }

  @Test
  public void validateZipDirectoryValidNatural() throws IOException {
    ZipEntry entry = new ZipEntry("somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDir, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirWithTrailingSlash, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirInPrivateMappedDir, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirInPrivateMappedDirWithTrailingSlash, entry);
  }

  @Test
  public void validateZipDirectoryValidSelfref() throws IOException {
    ZipEntry entry = new ZipEntry("./somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDir, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirWithTrailingSlash, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirInPrivateMappedDir, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirInPrivateMappedDirWithTrailingSlash, entry);
  }

  @Test
  public void validateZipDirectoryValidUpdirThenCorrectDir() throws IOException {
    ZipEntry entry = new ZipEntry("../tmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDir, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirWithTrailingSlash, entry);
  }

  @Test
  public void validateZipDirectoryValidUpdirMoreTheCorrectDir() throws IOException {
    ZipEntry entry = new ZipEntry("../../../../../tmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDir, entry);
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDirWithTrailingSlash, entry);
  }

  @Test
  public void validateZipDirectoryValidNestedUpdirThenCorrectDir() throws IOException {
    ZipEntry entry = new ZipEntry("../nestedTmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockNestedTempDir, entry);
  }

  @Test
  public void validateZipDirectoryValidUpdirMoreThanNestedCorrectDir() throws IOException {
    ZipEntry entry = new ZipEntry("../../../../baseTmpDir/nestedTmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockNestedTempDir, entry);
  }

  @Test
  public void validateZipDirectoryZipSlipTestUpdirMoreThanNestedCorrectDir() throws IOException {
    // This expected to pass, because this will eventually resolve to /baseTmpDir/nestedTmpDir/somedir/somefile,
    // which is a legal path within the base directory.
    ZipEntry entry = new ZipEntry("../../../../../baseTmpDir/nestedTmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockNestedTempDir, entry);
  }

  @Test
  public void validateZipDirectoryZipSlipTestUpdirMoreThanNestedCorrectDir2() throws IOException {
    // This expected to pass, because this will eventually resolve to /baseTmpDir/nestedTmpDir/somedir/somefile,
    // which is a legal path within the base directory.
    ZipEntry entry = new ZipEntry("../../../../../../../../../../../baseTmpDir/nestedTmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockNestedTempDir, entry);
  }

  @Test(expected = IOException.class)
  public void validateZipDirectoryZipSlipTestUpdirMoreThanNestedCorrectDirFinalDirDifferent() throws IOException {
    // This expected to fail, because this would resolve to /baseTmpDir/nestedTmpDir2/somedir/somefile,
    // which differs from /baseTmpDir/nestedTmpDir
    ZipEntry entry = new ZipEntry("../../../../../baseTmpDir/nestedTmpDir2/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockNestedTempDir, entry);
  }

  @Test(expected = IOException.class)
  public void validateZipDirectoryZipSlipTestNested() throws IOException {
    ZipEntry entry = new ZipEntry("../tmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockNestedTempDir, entry);
  }

  @Test(expected = IOException.class)
  public void validateZipDirectoryZipSlipTestNestedUpdirMore() throws IOException {
    ZipEntry entry = new ZipEntry("../../../../../tmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockNestedTempDir, entry);
  }

  @Test(expected = IOException.class)
  public void validateZipDirectoryZipSlipTest() throws IOException {
    ZipEntry entry = new ZipEntry("../somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDir, entry);
  }

  @Test(expected = IOException.class)
  public void validateZipDirectoryZipSlipTestUpMoreDirs() throws IOException {
    ZipEntry entry = new ZipEntry("../../../../../somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDir, entry);
  }

  @Test
  public void validateZipDirectoryZipSlipTestUpDirsMispelledCorrectDir() throws IOException {
    // This expected to pass, because this will eventually resolve to /tmpDir/somedir/somefile
    // which is a legal path within the base directory.
    ZipEntry entry = new ZipEntry("../../../..//tmpDir/somedir/somefile");
    NativeLibPluginClassLoader.validateZipDirectory(mockTempDir, entry);
  }
}
