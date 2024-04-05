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
package com.dremio.security;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.attribute.PosixFilePermission.GROUP_READ;
import static java.nio.file.attribute.PosixFilePermission.OTHERS_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE;
import static java.nio.file.attribute.PosixFilePermission.OWNER_READ;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.config.DremioConfig;
import com.dremio.test.DremioTest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.EnumSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@code SecurityFolder} */
public class TestSecurityFolder extends DremioTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testNewDirectoryDirectory() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath().resolve("non-existing-directory");
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final EnumSet<PosixFilePermission> permissions =
        EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ);

    @SuppressWarnings("unused")
    final SecurityFolder securityFolder = SecurityFolder.of(config);
    final Path securityPath = folder.resolve("security");

    assertTrue(Files.exists(securityPath));
    assertEquals(permissions, Files.getPosixFilePermissions(folder));
  }

  @Test
  public void testExistingDirectory() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final EnumSet<PosixFilePermission> permissions =
        EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ);
    final Path securityPath = folder.resolve("security");
    Files.createDirectories(securityPath, PosixFilePermissions.asFileAttribute(permissions));

    @SuppressWarnings("unused")
    SecurityFolder securityFolder = SecurityFolder.of(config);
    assertTrue(Files.exists(securityPath));
    assertEquals(permissions, Files.getPosixFilePermissions(securityPath));
  }

  @Test(expected = GeneralSecurityException.class)
  public void testInvalidExistingDirectory() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final EnumSet<PosixFilePermission> permissions =
        EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ, GROUP_READ);
    final Path securityPath = folder.resolve("security");
    Files.createDirectories(securityPath, PosixFilePermissions.asFileAttribute(permissions));

    @SuppressWarnings("unused")
    SecurityFolder securityFolder = SecurityFolder.of(config);
  }

  @Test
  public void testExists() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final SecurityFolder securityFolder = SecurityFolder.of(config);

    final Path securityPath = folder.resolve("security");
    Files.createFile(securityPath.resolve("testfile"));
    Files.createDirectories(securityPath.resolve("testdirectory"));
    assertTrue(securityFolder.exists("testfile"));
    assertTrue(securityFolder.exists("testdirectory"));
    assertFalse(securityFolder.exists("testnonexisting"));
  }

  @Test
  public void testResolve() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final SecurityFolder securityFolder = SecurityFolder.of(config);

    final Path securityPath = folder.resolve("security");
    Files.createFile(securityPath.resolve("testfile"));
    Files.createDirectories(securityPath.resolve("testdirectory"));
    assertEquals(securityPath.resolve("testfile"), securityFolder.resolve("testfile"));
    assertEquals(securityPath.resolve("testdirectory"), securityFolder.resolve("testdirectory"));
    assertEquals(
        securityPath.resolve("testnonexisting"), securityFolder.resolve("testnonexisting"));
  }

  @Test
  public void testSecureInputStream() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final SecurityFolder securityFolder = SecurityFolder.of(config);

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    Files.write(testfilePath, Arrays.asList("foo"), UTF_8);
    Files.setPosixFilePermissions(testfilePath, EnumSet.of(OWNER_WRITE, OWNER_READ));

    try (InputStream inputStream = securityFolder.newSecureInputStream("testfile");
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, UTF_8))) {
      assertEquals("foo", reader.readLine());
      assertEquals(null, reader.readLine()); // End of file
    }
  }

  @Test(expected = GeneralSecurityException.class)
  public void testSecureInputStreamInvalidPermissions()
      throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final SecurityFolder securityFolder = SecurityFolder.of(config);

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    Files.write(testfilePath, Arrays.asList("foo"), UTF_8);
    Files.setPosixFilePermissions(
        testfilePath, EnumSet.of(OWNER_WRITE, OWNER_READ, GROUP_READ, OTHERS_READ));

    try (InputStream inputStream = securityFolder.newSecureInputStream("testfile")) {
      assertNotNull(inputStream);
    }
  }

  @Test(expected = NoSuchFileException.class)
  public void testSecureInputStreamFileNotFound() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final SecurityFolder securityFolder = SecurityFolder.of(config);

    try (InputStream inputStream = securityFolder.newSecureInputStream("testfile")) {
      assertNotNull(inputStream);
    }
  }

  @Test
  public void testSecureOutputStream() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
            securityFolder.newSecureOutputStream(
                "testfile", SecurityFolder.OpenOption.CREATE_OR_WRITE);
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(outputStream, UTF_8))) {
      printWriter.println("bar");
    }

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    assertEquals(Arrays.asList("bar"), Files.readAllLines(testfilePath, UTF_8));
    assertEquals(EnumSet.of(OWNER_WRITE, OWNER_READ), Files.getPosixFilePermissions(testfilePath));
  }

  @Test
  public void testSecureOutputStreamCreateOrWriteExistingFile()
      throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    Files.createDirectories(
        securityPath,
        PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ)));
    Files.write(testfilePath, Arrays.asList("foo"), UTF_8);
    Files.setPosixFilePermissions(testfilePath, EnumSet.of(OWNER_WRITE, OWNER_READ));

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
            securityFolder.newSecureOutputStream(
                "testfile", SecurityFolder.OpenOption.CREATE_OR_WRITE);
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(outputStream, UTF_8))) {
      printWriter.println("bar");
    }

    assertEquals(Arrays.asList("bar"), Files.readAllLines(testfilePath, UTF_8));
    assertEquals(EnumSet.of(OWNER_WRITE, OWNER_READ), Files.getPosixFilePermissions(testfilePath));
  }

  @Test(expected = GeneralSecurityException.class)
  public void testSecureOutputStreamCreateOrWriteExistingFileInvalidPermissions()
      throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    Files.createDirectories(
        securityPath,
        PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ)));
    Files.write(testfilePath, Arrays.asList("foo"), UTF_8);
    Files.setPosixFilePermissions(
        testfilePath, EnumSet.of(OWNER_WRITE, OWNER_READ, GROUP_READ, OTHERS_READ));

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
        securityFolder.newSecureOutputStream(
            "testfile", SecurityFolder.OpenOption.CREATE_OR_WRITE)) {
      assertNotNull(outputStream);
    }
  }

  @Test(expected = NoSuchFileException.class)
  public void testSecureOutputStreamNoCreateNoFile() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
        securityFolder.newSecureOutputStream("testfile", SecurityFolder.OpenOption.NO_CREATE)) {
      assertNotNull(outputStream);
    }
  }

  @Test
  public void testSecureOutputStreamNoCreateExistingFile()
      throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    Files.createDirectories(
        securityPath,
        PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ)));
    Files.write(testfilePath, Arrays.asList("foo"), UTF_8);
    Files.setPosixFilePermissions(testfilePath, EnumSet.of(OWNER_WRITE, OWNER_READ));

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
            securityFolder.newSecureOutputStream("testfile", SecurityFolder.OpenOption.NO_CREATE);
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(outputStream, UTF_8))) {
      printWriter.println("bar");
    }

    assertEquals(Arrays.asList("bar"), Files.readAllLines(testfilePath, UTF_8));
    assertEquals(EnumSet.of(OWNER_WRITE, OWNER_READ), Files.getPosixFilePermissions(testfilePath));
  }

  @Test(expected = GeneralSecurityException.class)
  public void testSecureOutputStreamNoCreateExistingFileInvalidPermissions()
      throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    Files.createDirectories(
        securityPath,
        PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ)));
    Files.write(testfilePath, Arrays.asList("foo"), UTF_8);
    Files.setPosixFilePermissions(
        testfilePath, EnumSet.of(OWNER_WRITE, OWNER_READ, GROUP_READ, OTHERS_READ));

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
        securityFolder.newSecureOutputStream("testfile", SecurityFolder.OpenOption.NO_CREATE)) {
      assertNotNull(outputStream);
    }
  }

  @Test
  public void testSecureOutputStreamCreateOnly() throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
            securityFolder.newSecureOutputStream(
                "testfile", SecurityFolder.OpenOption.CREATE_OR_WRITE);
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(outputStream, UTF_8))) {
      printWriter.println("bar");
    }

    assertEquals(Arrays.asList("bar"), Files.readAllLines(testfilePath, UTF_8));
    assertEquals(EnumSet.of(OWNER_WRITE, OWNER_READ), Files.getPosixFilePermissions(testfilePath));
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testSecureOutputStreamCreateOnlyExistingFile()
      throws IOException, GeneralSecurityException {
    final Path folder = temporaryFolder.newFolder().toPath();
    final DremioConfig config =
        DEFAULT_DREMIO_CONFIG.withValue(DremioConfig.LOCAL_WRITE_PATH_STRING, folder.toString());

    final Path securityPath = folder.resolve("security");
    final Path testfilePath = securityPath.resolve("testfile");
    Files.createDirectories(
        securityPath,
        PosixFilePermissions.asFileAttribute(EnumSet.of(OWNER_EXECUTE, OWNER_WRITE, OWNER_READ)));
    Files.write(testfilePath, Arrays.asList("foo"), UTF_8);
    Files.setPosixFilePermissions(testfilePath, EnumSet.of(OWNER_WRITE, OWNER_READ));

    final SecurityFolder securityFolder = SecurityFolder.of(config);
    try (OutputStream outputStream =
        securityFolder.newSecureOutputStream("testfile", SecurityFolder.OpenOption.CREATE_ONLY)) {
      assertNotNull(outputStream);
    }
  }
}
