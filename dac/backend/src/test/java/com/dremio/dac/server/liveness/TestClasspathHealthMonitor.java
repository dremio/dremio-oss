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
package com.dremio.dac.server.liveness;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.test.DremioTest;

/**
 * Tests for ClasspathHealthMonitor
 */
public class TestClasspathHealthMonitor extends DremioTest {

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void checkIsHealthy() throws IOException {
    Path folder1 = temporaryFolder.newFolder("abc").toPath();
    Path folder2 = temporaryFolder.newFolder("xyz").toPath();

    ClasspathHealthMonitor classpathHealthMonitor =  ClasspathHealthMonitor.newInstance(Stream.of(folder1, folder2));
    assertTrue(classpathHealthMonitor.isHealthy());
  }

  @Test
  public void checkIsHealthyWithMissingFolder() throws IOException {
    Path folder1 = temporaryFolder.newFolder("abc").toPath();
    Path folder2 = temporaryFolder.newFolder("xyz").toPath();
    Path folder3 = Paths.get("/dev/null/nonexistent");

    ClasspathHealthMonitor classpathHealthMonitor = ClasspathHealthMonitor.newInstance(Stream.of(folder1, folder2, folder3));
    assertTrue(classpathHealthMonitor.isHealthy());
  }

  @Test
  public void checkIsHealthyWithMissingFile() throws IOException {
    Path folder1 = temporaryFolder.newFolder("abc").toPath();
    Path folder2 = temporaryFolder.newFolder("xyz").toPath();
    Path folder3 = temporaryFolder.newFolder("foo").toPath();

    ClasspathHealthMonitor classpathHealthMonitor =  ClasspathHealthMonitor.newInstance(Stream.of(folder1, folder2, folder3.resolve("text.txt")));
    assertTrue(classpathHealthMonitor.isHealthy());

    Files.delete(folder3);
    assertTrue(classpathHealthMonitor.isHealthy());
  }

  @Test
  public void checkIsNotHealthyDirectoryNonExecutable() throws IOException {
    Path folder1 = temporaryFolder.newFolder("abc").toPath();
    Path folder2 = temporaryFolder.newFolder("xyz").toPath();

    ClasspathHealthMonitor classpathHealthMonitor = ClasspathHealthMonitor.newInstance(Stream.of(folder1, folder2));
    assertTrue(classpathHealthMonitor.isHealthy());

    // Remove executable atttribute to folder
    Set<PosixFilePermission> attributes = Files.getPosixFilePermissions(folder2);
    attributes.remove(PosixFilePermission.OWNER_EXECUTE);
    Files.setPosixFilePermissions(folder2, attributes);

    assertFalse(classpathHealthMonitor.isHealthy());
  }

  @Test
  public void checkIsNotHealthyDirectoryDeleted() throws IOException {
    Path folder1 = temporaryFolder.newFolder("abc").toPath();
    Path folder2 = temporaryFolder.newFolder("xyz").toPath();

    ClasspathHealthMonitor classpathHealthMonitor = ClasspathHealthMonitor.newInstance(Stream.of(folder1, folder2));
    assertTrue(classpathHealthMonitor.isHealthy());
    Files.delete(folder2);
    assertFalse(classpathHealthMonitor.isHealthy());
  }
}
